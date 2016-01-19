package com.twitter.finagle.mux.transport

import com.twitter.finagle.tracing.{SpanId, TraceId, Flags}
import com.twitter.finagle.{Dtab, Dentry, NameTree, Path}
import com.twitter.io.Charsets
import com.twitter.util.{Duration, Time}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers, ReadOnlyChannelBuffer}

/**
 * Indicates that encoding or decoding of a Mux message failed.
 * Reason for failure should be provided by the `why` string.
 */
case class BadMessageException(why: String) extends Exception(why)

/**
 * Documentation details are in the [[com.twitter.finagle.mux]] package object.
 */
private[twitter] sealed trait Message {
  /**
   * Values should correspond to the constants defined in
   * [[com.twitter.finagle.mux.Message.Types]]
   */
  def typ: Byte

  /** Only 3 of its bytes are used. */
  def tag: Int

  def buf: ChannelBuffer
}

private[twitter] object Message {
  object Types {
    // Application messages:
    val Treq = 1: Byte
    val Rreq = -1: Byte

    val Tdispatch = 2: Byte
    val Rdispatch = -2: Byte

    // Control messages:
    val Tdrain = 64: Byte
    val Rdrain = -64: Byte
    val Tping = 65: Byte
    val Rping = -65: Byte

    val Tdiscarded = 66: Byte
    val Tlease = 67: Byte

    val Rerr = -128: Byte

    // Old implementation flukes.
    val BAD_Tdiscarded = -62: Byte
    val BAD_Rerr = 127: Byte
  }

  val MarkerTag = 0
  // We reserve a tag for a default ping message to that we
  // can cache a full ping message and avoid encoding it
  // every time.
  val PingTag = 1
  val MinTag = PingTag+1
  val MaxTag = (1 << 23) - 1
  val TagMSB = (1 << 23)

  private def mkByte(b: Byte) =
    ChannelBuffers.unmodifiableBuffer(ChannelBuffers.wrappedBuffer(Array(b)))

  private val bufOfChar = Array[ChannelBuffer](
    mkByte(0), mkByte(1), mkByte(2))

  abstract class EmptyMessage extends Message {
    def buf = ChannelBuffers.EMPTY_BUFFER
  }

  abstract class MarkerMessage extends Message {
    def tag = 0
  }

  object Treq {
    object Keys {
      val TraceId = 1
      val TraceFlag = 2
    }
  }

  /**
   * A transmit request message.
   *
   * Note, Treq messages are deprecated in favor of [[Tdispatch]] and will likely
   * be removed in a future version of mux.
   */
  case class Treq(tag: Int, traceId: Option[TraceId], req: ChannelBuffer) extends Message {
    import Treq._
    def typ = Types.Treq
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

  /**
   * A reply to a `Treq` message.
   *
   * Note, Rreq messages are deprecated in favor of [[Rdispatch]] and will likely
   * be removed in a future version of mux.
   */
  abstract class Rreq(rreqType: Byte, body: ChannelBuffer) extends Message {
    def typ = Types.Rreq
    lazy val buf = ChannelBuffers.wrappedBuffer(bufOfChar(rreqType), body)
  }

  case class RreqOk(tag: Int, reply: ChannelBuffer) extends Rreq(0, reply)
  case class RreqError(tag: Int, error: String) extends Rreq(1, encodeString(error))
  case class RreqNack(tag: Int) extends Rreq(2, ChannelBuffers.EMPTY_BUFFER)

  private[this] val noBytes = Array.empty[Byte]

  case class Tdispatch(
      tag: Int,
      contexts: Seq[(ChannelBuffer, ChannelBuffer)],
      dst: Path,
      dtab: Dtab,
      req: ChannelBuffer)
    extends Message {
    def typ = Types.Tdispatch
    lazy val buf = {
      // first, compute how large the message header is (in 'n')
      var n = 2
      var seq = contexts
      while (seq.nonEmpty) {
        // Note: here and below we don't use the scala dereferencing sugar of
        // `val (k, v) = seq.head` as that caused unnecessary Tuple2 allocations.
        seq.head match { case (k, v) =>
          n += 2 + k.readableBytes + 2 + v.readableBytes
        }
        seq = seq.tail
      }

      val dstbytes = if (dst.isEmpty) noBytes else dst.show.getBytes(Charsets.Utf8)
      n += 2 + dstbytes.length
      n += 2

      val dtabbytes = new Array[(Array[Byte], Array[Byte])](dtab.size)
      var dtabidx = 0
      var i = 0
      while (i < dtab.length) {
        val dentry = dtab(i)
        val srcbytes = dentry.prefix.show.getBytes(Charsets.Utf8)
        val treebytes = dentry.dst.show.getBytes(Charsets.Utf8)

        n += srcbytes.length + 2 + treebytes.length + 2

        dtabbytes(dtabidx) = (srcbytes, treebytes)
        dtabidx += 1
        i += 1
      }

      // then, allocate and populate the header
      val hd = ChannelBuffers.dynamicBuffer(n)
      hd.writeShort(contexts.length)
      seq = contexts
      while (seq.nonEmpty) {
        // TODO: it may or may not make sense
        // to do zero-copy here.
        seq.head match { case (k, v) =>
          hd.writeShort(k.readableBytes)
          hd.writeBytes(k.slice())
          hd.writeShort(v.readableBytes)
          hd.writeBytes(v.slice())
        }
        seq = seq.tail
      }

      hd.writeShort(dstbytes.length)
      hd.writeBytes(dstbytes)

      hd.writeShort(dtab.size)
      dtabidx = 0
      while (dtabidx != dtabbytes.length) {
        dtabbytes(dtabidx) match { case (srcbytes, treebytes) =>
          hd.writeShort(srcbytes.length)
          hd.writeBytes(srcbytes)
          hd.writeShort(treebytes.length)
          hd.writeBytes(treebytes)
        }
        dtabidx += 1
      }

      ChannelBuffers.wrappedBuffer(hd, req)
    }
  }

  /** A reply to a `Tdispatch` message */
  abstract class Rdispatch(
      status: Byte,
      contexts: Seq[(ChannelBuffer, ChannelBuffer)],
      body: ChannelBuffer)
    extends Message {
    def typ = Types.Rdispatch
    lazy val buf = {
      var n = 1+2
      var seq = contexts
      while (seq.nonEmpty) {
        seq.head match { case (k, v) =>
          n += 2+k.readableBytes+2+v.readableBytes
        }
        seq = seq.tail
      }

      val hd = ChannelBuffers.buffer(n)
      hd.writeByte(status)
      hd.writeShort(contexts.length)
      seq = contexts
      while (seq.nonEmpty) {
        seq.head match { case (k, v) =>
          hd.writeShort(k.readableBytes)
          hd.writeBytes(k.slice())
          hd.writeShort(v.readableBytes)
          hd.writeBytes(v.slice())
        }
        seq = seq.tail
      }

      ChannelBuffers.wrappedBuffer(hd, body)
    }
  }

  case class RdispatchOk(
      tag: Int,
      contexts: Seq[(ChannelBuffer, ChannelBuffer)],
      reply: ChannelBuffer)
    extends Rdispatch(0, contexts, reply)

  case class RdispatchError(
      tag: Int,
      contexts: Seq[(ChannelBuffer, ChannelBuffer)],
      error: String)
    extends Rdispatch(1, contexts, encodeString(error))

  case class RdispatchNack(
      tag: Int,
      contexts: Seq[(ChannelBuffer, ChannelBuffer)])
    extends Rdispatch(2, contexts, ChannelBuffers.EMPTY_BUFFER)

  /** Indicates to the client to stop sending new requests. */
  case class Tdrain(tag: Int) extends EmptyMessage { def typ = Types.Tdrain }

  /** Response from the client to a `Tdrain` message */
  case class Rdrain(tag: Int) extends EmptyMessage { def typ = Types.Rdrain }

  /** Used to check liveness */
  case class Tping(tag: Int) extends EmptyMessage { def typ = Types.Tping }

  /**
   * We pre-encode a ping message with the reserved ping tag
   * (PingTag) in order to avoid re-encoding this frequently sent
   * message. Since it uses ChannelBuffers, it maintains a read
   * cursor, and thus it is important that it is not used
   * concurrently. This happens to agree with the natural way you'd
   * use it, since a client can only have one outstanding ping per
   * tag.
   */
  final class PreEncodedTping extends Message {
    def typ = ???
    def tag = ???

    private[this] val cb = new ReadOnlyChannelBuffer(encode(Tping(PingTag)))
    cb.markReaderIndex()

    def buf = {
      cb.resetReaderIndex()
      cb
    }
  }

  /** Response to a `Tping` message */
  case class Rping(tag: Int) extends EmptyMessage { def typ = Types.Rping }

  /** Indicates that the corresponding T message produced an error. */
  case class Rerr(tag: Int, error: String) extends Message {
    // Use the old Rerr type in a transition period so that we
    // can be reasonably sure we remain backwards compatible with
    // old servers.

    def typ = Types.BAD_Rerr
    lazy val buf = encodeString(error)
  }

  /**
   * Indicates that the `Treq` with the tag indicated by `which` has been discarded
   * by the client.
   */
  case class Tdiscarded(which: Int, why: String)
      // Use the old Tdiscarded type in a transition period so that we
      // can be reasonably sure we remain backwards compatible with
      // old servers.
      extends MarkerMessage {
    def typ = Types.BAD_Tdiscarded
    lazy val buf = ChannelBuffers.wrappedBuffer(
      ChannelBuffers.wrappedBuffer(
        Array[Byte]((which >> 16 & 0xff).toByte,
          (which >> 8 & 0xff).toByte,
          (which & 0xff).toByte)),
      encodeString(why))
  }

  object Tlease {
    val MinLease = Duration.Zero
    val MaxLease = Duration.fromMilliseconds((1L << 32) - 1) // Unsigned Int max value

    val MillisDuration: Byte = 0

    def apply(howLong: Duration): Tlease = {
      require(howLong >= MinLease && howLong <= MaxLease, "lease out of range")
      Tlease(MillisDuration, howLong.inMilliseconds)
    }
    def apply(end: Time): Tlease = Tlease(1, end.sinceEpoch.inMilliseconds)
  }

  case class Tlease(unit: Byte, howLong: Long) extends MarkerMessage {
    def typ = Types.Tlease
    lazy val buf = {
      val b = ChannelBuffers.buffer(9)
      b.writeByte(unit)
      b.writeLong(howLong)
      b
    }
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

  object ControlMessage {
    // TODO: Update this extractor in the event that we "fix" the control
    // message flukes by removing backwards compatibility.
    def unapply(m: Message): Option[Int] =
      if (math.abs(m.typ) >= 64 || m.typ == Types.BAD_Tdiscarded)
        Some(m.tag)
      else None
  }

  def decodeUtf8(buf: ChannelBuffer): String =
    decodeUtf8(buf, buf.readableBytes)

  def decodeUtf8(buf: ChannelBuffer, n: Int): String = {
    val arr = new Array[Byte](n)
    buf.readBytes(arr)
    new String(arr, Charsets.Utf8)
  }

  def encodeString(str: String) =
    ChannelBuffers.wrappedBuffer(str.getBytes(Charsets.Utf8))

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
          trace3 = Some((
            SpanId(buf.readLong()),  // spanId
            SpanId(buf.readLong()),  // parentId
            SpanId(buf.readLong()))  // traceId
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

    val id = trace3 match {
      case Some((spanId, parentId, traceId)) =>
        Some(TraceId(Some(traceId), Some(parentId), spanId, None, Flags(traceFlags)))
      case None => None
    }

    Treq(tag, id, buf.slice())
  }

  private def decodeContexts(buf: ChannelBuffer): Seq[(ChannelBuffer, ChannelBuffer)] = {
    val n = buf.readUnsignedShort()
    if (n == 0)
      return Nil

    val contexts = new Array[(ChannelBuffer, ChannelBuffer)](n)
    var i = 0
    while (i < n) {
      val nk = buf.readUnsignedShort()
      val k = buf.readSlice(nk)
      val nv = buf.readUnsignedShort()
      val v = buf.readSlice(nv)
      contexts(i) = (k, v)
      i += 1
    }
    contexts
  }

  private def decodeTdispatch(tag: Int, buf: ChannelBuffer) = {
    val contexts = decodeContexts(buf)

    val ndst = buf.readUnsignedShort()
    // Path.read("") fails, so special case empty-dst.
    val dst =
      if (ndst == 0) Path.empty
      else Path.read(decodeUtf8(buf.readSlice(ndst)))

    val nd = buf.readUnsignedShort()
    val dtab = if (nd == 0) Dtab.empty else {
      var i = 0
      val delegations = new Array[Dentry](nd)
      while (i < nd) {
        val src = decodeUtf8(buf, buf.readUnsignedShort())
        val dst = decodeUtf8(buf, buf.readUnsignedShort())
        delegations(i) = Dentry(Path.read(src), NameTree.read(dst))
        i += 1
      }
      Dtab(delegations)
    }

    Tdispatch(tag, contexts, dst, dtab, buf.slice())
  }

  private def decodeRdispatch(tag: Int, buf: ChannelBuffer) = {
    val status = buf.readByte()
    val contexts = decodeContexts(buf)
    status match {
      case 0 => RdispatchOk(tag, contexts, buf.slice())
      case 1 => RdispatchError(tag, contexts, decodeUtf8(buf))
      case 2 => RdispatchNack(tag, contexts)
      case _ => throw BadMessageException("invalid Rdispatch status")
    }
  }

  private def decodeRreq(tag: Int, buf: ChannelBuffer) = {
    if (buf.readableBytes < 1)
      throw BadMessageException("short Rreq")
    buf.readByte() match {
      case 0 => RreqOk(tag, buf.slice())
      case 1 => RreqError(tag, decodeUtf8(buf))
      case 2 => RreqNack(tag)
      case _ => throw BadMessageException("invalid Rreq status")
    }
  }

  private def decodeTdiscarded(buf: ChannelBuffer) = {
    if (buf.readableBytes < 3)
      throw BadMessageException("short Tdiscarded message")
    val which = ((buf.readByte() & 0xff) << 16) |
      ((buf.readByte() & 0xff) << 8) |
      (buf.readByte() & 0xff)
    Tdiscarded(which, decodeUtf8(buf))
  }

  private def decodeTlease(buf: ChannelBuffer) = {
    if (buf.readableBytes < 9)
      throw BadMessageException("short Tlease message")
    val unit: Byte = buf.readByte()
    val howMuch: Long = buf.readLong()
    Tlease(unit, howMuch)
  }

  def decode(buf: ChannelBuffer): Message = {
    if (buf.readableBytes < 4)
      throw BadMessageException("short message")
    val head = buf.readInt()
    def typ = (head >> 24 & 0xff).toByte
    val tag = head & 0x00ffffff
    typ match {
      case Types.Treq => decodeTreq(tag, buf)
      case Types.Rreq => decodeRreq(tag, buf)
      case Types.Tdispatch => decodeTdispatch(tag, buf)
      case Types.Rdispatch => decodeRdispatch(tag, buf)
      case Types.Tdrain => Tdrain(tag)
      case Types.Rdrain => Rdrain(tag)
      case Types.Tping => Tping(tag)
      case Types.Rping => Rping(tag)
      case Types.Rerr | Types.BAD_Rerr => Rerr(tag, decodeUtf8(buf))
      case Types.Tdiscarded | Types.BAD_Tdiscarded => decodeTdiscarded(buf)
      case Types.Tlease => decodeTlease(buf)
      case bad => throw BadMessageException("bad message type: %d [tag=%d]".format(bad, tag))
    }
  }

  def encode(msg: Message): ChannelBuffer = msg match {
    case m: PreEncodedTping => msg.buf
    case m: Message =>
      if (m.tag < MarkerTag || (m.tag & ~TagMSB) > MaxTag)
        throw new BadMessageException("invalid tag number %d".format(m.tag))

      val head = Array[Byte](
        m.typ,
        (m.tag >> 16 & 0xff).toByte,
        (m.tag >> 8 & 0xff).toByte,
        (m.tag & 0xff).toByte
      )

      ChannelBuffers.wrappedBuffer(
        ChannelBuffers.wrappedBuffer(head), m.buf)
  }
}