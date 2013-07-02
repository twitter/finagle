package com.twitter.finagle.mux

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.tracing.{SpanId, TraceId, Flags}

case class BadMessageException(why: String) extends Exception(why)

// TODO: when the new com.twitter.codec.Codec arrives, define Message
// parsing as a bijection between ChannelBuffers and Message.

private[finagle] sealed trait Message {
  val typ: Byte
  val tag: Int
  val buf: ChannelBuffer
}

private[finagle] object Message {
  object Types {
    // Application messages:
    val Treq = 1: Byte
    val Rreq = -1: Byte

    // Control messages:
    val Tdrain = 64: Byte
    val Rdrain = -64: Byte
    val Tping = 65: Byte
    val Rping = -63: Byte

    val Tdiscarded = -62: Byte

    // Could be either:
    val Rerr = 127: Byte
  }

  val MarkerTag = 0
  val MinTag = 1
  val MaxTag = (1<<23)-1

  private val bufOfChar = Array[ChannelBuffer](
    ChannelBuffers.unmodifiableBuffer(ChannelBuffers.wrappedBuffer(Array[Byte](0))),
    ChannelBuffers.unmodifiableBuffer(ChannelBuffers.wrappedBuffer(Array[Byte](1))),
    ChannelBuffers.unmodifiableBuffer(ChannelBuffers.wrappedBuffer(Array[Byte](2))))

  abstract class EmptyMessage(val typ: Byte) extends Message {
    val buf = ChannelBuffers.EMPTY_BUFFER
  }
  abstract class MarkerMessage(val typ: Byte) extends Message {
    val tag = 0
  }

  object Treq {
    object Keys {
      val TraceId = 1
      val TraceFlag = 2
    }
  }

  case class Treq(tag: Int, traceId: Option[TraceId], req: ChannelBuffer) extends Message {
    import Treq._
    val typ = Types.Treq
    lazy val buf = {
      val header = traceId match {
        // Currently we require the 3-tuple, but this is not
        // necessarily required.
        case Some(traceId) =>
          val hd = ChannelBuffers.buffer(1+1+1+24+1+1+1)
          hd.writeByte(2) // 2 entries

          hd.writeByte(Keys.TraceId) // key 0 (traceid)
          hd.writeByte(24) // key 0 size
          hd.writeLong(traceId.spanId.toLong)
          hd.writeLong(traceId.parentId.toLong)
          hd.writeLong(traceId.traceId.toLong)

          hd.writeByte(Keys.TraceFlag) // key 1 (traceflag)
          hd.writeByte(1) // key 1 size
          hd.writeByte(traceId.flags.toLong.toByte)

          hd

        case None =>
          bufOfChar(0) // 0 keys
      }

      ChannelBuffers.wrappedBuffer(header, req)
    }
  }

  abstract class Rreq(rreqType: Byte, body: ChannelBuffer) extends Message {
    val typ = Types.Rreq
    lazy val buf = ChannelBuffers.wrappedBuffer(bufOfChar(rreqType), body)
  }

  case class RreqOk(tag: Int, reply: ChannelBuffer) extends Rreq(0, reply)
  case class RreqError(tag: Int, error: String) extends Rreq(1, encodeString(error))
  case class RreqNack(tag: Int) extends Rreq(2, ChannelBuffers.EMPTY_BUFFER)

  case class Tdrain(tag: Int) extends EmptyMessage(Types.Tdrain)
  case class Rdrain(tag: Int) extends EmptyMessage(Types.Rdrain)
  case class Tping(tag: Int) extends EmptyMessage(Types.Tping)
  case class Rping(tag: Int) extends EmptyMessage(Types.Rping)
  case class Rerr(tag: Int, error: String) extends Message {
    val typ = Types.Rerr
    lazy val buf = encodeString(error)
  }

  case class Tdiscarded(which: Int, why: String) extends MarkerMessage(Types.Tdiscarded) {
    lazy val buf = ChannelBuffers.wrappedBuffer(
      ChannelBuffers.wrappedBuffer(
        Array[Byte]((which>>16 & 0xff).toByte, (which>>8 & 0xff).toByte, (which & 0xff).toByte)),
      encodeString(why))
  }

  object Tmessage {
    def unapply(m: Message): Option[Int] =
      if (m.typ > 0) Some(m.tag)
      else None
  }

  object Rmessage {
    def unapply(m: Message): Option[Int] =
      if (m.typ < 0) Some(m.tag)
      else None
  }

  def decodeString(buf: ChannelBuffer) = {
    val n = buf.readableBytes
    val arr = new Array[Byte](n)
    buf.readBytes(arr)
    new String(arr, CharsetUtil.UTF_8)
  }

  def encodeString(str: String) =
    ChannelBuffers.wrappedBuffer(str.getBytes(CharsetUtil.UTF_8))

  private def decodeTreq(tag: Int, buf: ChannelBuffer) = {
    if (buf.readableBytes < 1)
      throw BadMessageException("short Treq")

    var nkeys = buf.readByte().toInt
    if (nkeys < 0)
      throw BadMessageException("Treq: too many keys")

    var trace3: Option[(SpanId, SpanId, SpanId)] = None
    var traceFlags = 0L

    while (nkeys > 0) {
      if (buf.readableBytes < 2)
        throw BadMessageException("short Treq (header)")

      val key = buf.readByte()
      val vsize = buf.readByte().toInt match {
        case s if s < 0 => s + 256
        case s => s
      }

      if (buf.readableBytes < vsize)
        throw BadMessageException("short Treq (vsize)")

      // TODO: technically we should probably check for duplicate
      // keys, but for now, just pick the latest one.
      key match {
        case Treq.Keys.TraceId =>
          if (vsize != 24)
            throw BadMessageException("bad traceid size %d".format(vsize))
          trace3 = Some(
            SpanId(buf.readLong()),  // spanId
            SpanId(buf.readLong()),  // parentId
            SpanId(buf.readLong())  // traceId
          )

        case Treq.Keys.TraceFlag =>
          // We only know about bit=0, so discard
          // everything but the last byte
          if (vsize > 1)
            buf.readBytes(vsize-1)
          if (vsize > 0)
            traceFlags = buf.readByte().toLong

        case _ =>
          // discard:
          buf.readBytes(vsize)
      }

      nkeys -= 1
    }

    val id = trace3 map { case (spanId, parentId, traceId) =>
      TraceId(Some(traceId), Some(parentId), spanId, None, Flags(traceFlags))
    }

    Treq(tag, id, buf.duplicate())
  }

  private def decodeRreq(tag: Int, buf: ChannelBuffer) = {
    if (buf.readableBytes < 1)
      throw BadMessageException("short Rreq")
    buf.readByte() match {
      case 0 => RreqOk(tag, buf.duplicate())
      case 1 => RreqError(tag, decodeString(buf))
      case 2 => RreqNack(tag)
      case _ => throw BadMessageException("invalid Rreq status")
    }
  }

  private def decodeTdiscarded(buf: ChannelBuffer) = {
    if (buf.readableBytes < 3)
      throw BadMessageException("short Tdiscarded message")
    val which = ((buf.readByte() & 0xff)<<16) | ((buf.readByte() & 0xff)<<8) | (buf.readByte() & 0xff)
    Tdiscarded(which, decodeString(buf))
  }

  def decode(buf: ChannelBuffer): Message = {
    if (buf.readableBytes < 4)
      throw BadMessageException("short message")
    val head = buf.readInt()
    val typ = (head>>24 & 0xff).toByte
    val tag = head & 0x00ffffff
    typ match {
      case Types.Treq => decodeTreq(tag, buf)
      case Types.Rreq => decodeRreq(tag, buf)
      case Types.Tdrain => Tdrain(tag)
      case Types.Rdrain => Rdrain(tag)
      case Types.Tping => Tping(tag)
      case Types.Rping => Rping(tag)
      case Types.Rerr => Rerr(tag, decodeString(buf))
      case Types.Tdiscarded => decodeTdiscarded(buf)
      case bad => throw BadMessageException("bad message type: %d [tag=%d]".format(bad, tag))
    }
  }

  def encode(m: Message): ChannelBuffer = {
    if (m.tag < MarkerTag || m.tag > MaxTag || (m.tag == MarkerTag
        && !m.isInstanceOf[MarkerMessage]))
      throw new BadMessageException("invalid tag number %d".format(m.tag))

    val head = Array[Byte](
      m.typ,
      (m.tag>>16 & 0xff).toByte,
      (m.tag>>8 & 0xff).toByte,
      (m.tag & 0xff).toByte
    )

    ChannelBuffers.wrappedBuffer(
      ChannelBuffers.wrappedBuffer(head), m.buf)
  }
}
