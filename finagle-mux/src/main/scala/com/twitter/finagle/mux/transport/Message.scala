package com.twitter.finagle.mux.transport

import com.twitter.finagle.netty4.Bufs
import com.twitter.finagle.tracing.{Flags, SpanId, TraceId}
import com.twitter.finagle.{Dentry, Dtab, Failure, NameTree, Path}
import com.twitter.io.{Buf, ByteReader, ByteWriter}
import com.twitter.util.{Duration, Time}
import java.nio.charset.{StandardCharsets => Charsets}
import scala.collection.mutable.ArrayBuffer

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

  /**
   * The body of the message omitting size, typ, and tag.
   */
  def buf: Buf
}

private[twitter] object Message {


  object Types {

    private[mux] def isRefCounted(typ: Byte): Boolean =
      (typ == Types.Tping || typ == Types.Rping
        || typ == Types.Tdiscarded || typ == Types.Rdiscarded
        || typ == Types.Tdrain || typ == Types.Rdrain
        || typ == Types.Tlease)

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
    val Rdiscarded = -66: Byte

    val Tlease = 67: Byte

    val Tinit = 68: Byte
    val Rinit = -68: Byte

    val Rerr = -128: Byte

    // Old implementation flukes.
    val BAD_Tdiscarded = -62: Byte
    val BAD_Rerr = 127: Byte
  }

  object Tags {
    val MarkerTag = 0
    // We reserve a tag for a default ping message so that we
    // can cache a full ping message and avoid encoding it
    // every time.
    val PingTag = 1
    val MinTag = PingTag+1
    val MaxTag = (1 << 23) - 1
    val TagMSB = (1 << 23)

    def extractType(header: Int): Byte = (header >> 24 & 0xff).toByte
    def extractTag(header: Int): Int = header & 0x00ffffff
    def isFragment(tag: Int): Boolean = (tag >> 23 & 1) == 1
    def setMsb(tag: Int): Int = tag | TagMSB
  }

  private[mux] object ReplyStatus {
    val Ok: Byte = 0
    val Error: Byte = 1
    val Nack: Byte = 2
  }

  private def mkByte(b: Byte) = Buf.ByteArray.Owned(Array(b))

  private val bufOfChar = Array[Buf](mkByte(0), mkByte(1), mkByte(2))

  abstract class EmptyMessage extends Message {
    def buf: Buf = Buf.Empty
  }

  abstract class MarkerMessage extends Message {
    def tag = 0
  }

  private object Init {
    def encode(
      version: Short,
      headers: Seq[(Buf, Buf)]
    ): Buf = {
      var size = 2 // 2 bytes for version
      var iter = headers.iterator
      while (iter.hasNext) {
        val (k, v) = iter.next()
        // 8 bytes for length encoding of k, v
        size += 8 + k.length + v.length
      }
      val bw = ByteWriter.fixed(size)
      bw.writeShortBE(version)
      iter = headers.iterator
      while (iter.hasNext) {
        iter.next() match { case (k, v) =>
          bw.writeIntBE(k.length)
          bw.writeBytes(Buf.ByteArray.Owned.extract(k))
          bw.writeIntBE(v.length)
          bw.writeBytes(Buf.ByteArray.Owned.extract(v))
        }
      }
      bw.owned()
    }

    def decode(br: ByteReader): (Short, Seq[(Buf, Buf)]) = {
      val version = br.readShortBE()
      val headers = new ArrayBuffer[(Buf, Buf)]
      while (br.remaining > 0) {
        val k = br.readBytes(br.readIntBE())
        val v = br.readBytes(br.readIntBE())
        headers += (k -> v)
      }
      (version.toShort, headers)
    }
  }

  case class Tinit(
      tag: Int,
      version: Short,
      headers: Seq[(Buf, Buf)])
    extends Message {
    def typ: Byte = Types.Tinit
    lazy val buf: Buf = Init.encode(version, headers)
  }

  case class Rinit(
      tag: Int,
      version: Short,
      headers: Seq[(Buf, Buf)])
    extends Message {
    def typ: Byte = Types.Rinit
    lazy val buf: Buf = Init.encode(version, headers)
  }

  /**
   * A transmit request message.
   *
   * Note, Treq messages are deprecated in favor of [[Tdispatch]] and will likely
   * be removed in a future version of mux.
   */
  case class Treq(tag: Int, traceId: Option[TraceId], req: Buf) extends Message {
    import Treq._
    def typ = Types.Treq
    lazy val buf: Buf = {
      val header = traceId match {
        // Currently we require the 3-tuple, but this is not
        // necessarily required.
        case Some(traceId) =>
          val hd = ByteWriter.fixed(1+1+1+24+1+1+1)
          hd.writeByte(2) // 2 entries

          hd.writeByte(Keys.TraceId) // key 0 (traceid)
          hd.writeByte(24) // key 0 size
          hd.writeLongBE(traceId.spanId.toLong)
          hd.writeLongBE(traceId.parentId.toLong)
          hd.writeLongBE(traceId.traceId.toLong)

          hd.writeByte(Keys.TraceFlag) // key 1 (traceflag)
          hd.writeByte(1) // key 1 size
          hd.writeByte(traceId.flags.toLong.toByte)

          hd.owned()

        case None =>
          bufOfChar(0) // 0 keys
      }

      header.concat(req)
    }
  }

  object Treq {
    object Keys {
      val TraceId = 1
      val TraceFlag = 2
    }
  }

  /**
   * A reply to a `Treq` message.
   *
   * Note, Rreq messages are deprecated in favor of [[Rdispatch]] and will likely
   * be removed in a future version of mux.
   */
  abstract class Rreq(rreqType: Byte, body: Buf) extends Message {
    def typ = Types.Rreq
    lazy val buf: Buf = bufOfChar(rreqType).concat(body)
  }

  case class RreqOk(tag: Int, reply: Buf) extends Rreq(ReplyStatus.Ok, reply)
  case class RreqError(tag: Int, error: String) extends Rreq(ReplyStatus.Error, encodeString(error))
  case class RreqNack(tag: Int) extends Rreq(ReplyStatus.Nack, Buf.Empty)

  private[this] val noBytes = Array.empty[Byte]

  case class Tdispatch(
      tag: Int,
      contexts: Seq[(Buf, Buf)],
      dst: Path,
      dtab: Dtab,
      req: Buf)
    extends Message {
    def typ = Types.Tdispatch
    lazy val buf: Buf = {
      // first, compute how large the message header is (in 'n')
      var n = 2
      var iter = contexts.iterator
      while (iter.hasNext) {
        // Note: here and below we don't use the scala dereferencing sugar of
        // `val (k, v) = seq.head` as that caused unnecessary Tuple2 allocations.
        iter.next() match { case (k, v) =>
          n += 2 + k.length + 2 + v.length
        }
      }

      val dstbytes = if (dst.isEmpty) noBytes else dst.show.getBytes(Charsets.UTF_8)
      n += 2 + dstbytes.length
      n += 2

      val dtabbytes = new Array[(Array[Byte], Array[Byte])](dtab.size)
      var dtabidx = 0
      var i = 0
      while (i < dtab.length) {
        val dentry = dtab(i)
        val srcbytes = dentry.prefix.show.getBytes(Charsets.UTF_8)
        val treebytes = dentry.dst.show.getBytes(Charsets.UTF_8)

        n += srcbytes.length + 2 + treebytes.length + 2

        dtabbytes(dtabidx) = (srcbytes, treebytes)
        dtabidx += 1
        i += 1
      }

      // then, allocate and populate the header
      val hd = ByteWriter.fixed(n)
      hd.writeShortBE(contexts.length)
      iter = contexts.iterator
      while (iter.hasNext) {
        // TODO: it may or may not make sense
        // to do zero-copy here.
        iter.next() match { case (k, v) =>
          hd.writeShortBE(k.length)
          hd.writeBytes(Buf.ByteArray.Owned.extract(k))
          hd.writeShortBE(v.length)
          hd.writeBytes(Buf.ByteArray.Owned.extract(v))
        }
      }

      hd.writeShortBE(dstbytes.length)
      hd.writeBytes(dstbytes)

      hd.writeShortBE(dtab.size)
      dtabidx = 0
      while (dtabidx != dtabbytes.length) {
        dtabbytes(dtabidx) match { case (srcbytes, treebytes) =>
          hd.writeShortBE(srcbytes.length)
          hd.writeBytes(srcbytes)
          hd.writeShortBE(treebytes.length)
          hd.writeBytes(treebytes)
        }
        dtabidx += 1
      }

      hd.owned().concat(req)
    }
  }

  /** A reply to a `Tdispatch` message */
  abstract class Rdispatch(
      status: Byte,
      contexts: Seq[(Buf, Buf)],
      body: Buf)
    extends Message {
    def typ = Types.Rdispatch
    lazy val buf: Buf = {
      var n = 1+2
      var iter = contexts.iterator
      while (iter.hasNext) {
        iter.next() match { case (k, v) =>
          n += 2+k.length+2+v.length
        }
      }

      val hd = ByteWriter.fixed(n)
      hd.writeByte(status)
      hd.writeShortBE(contexts.length)
      iter = contexts.iterator
      while (iter.hasNext) {
        iter.next() match { case (k, v) =>
          hd.writeShortBE(k.length)
          hd.writeBytes(Buf.ByteArray.Owned.extract(k))
          hd.writeShortBE(v.length)
          hd.writeBytes(Buf.ByteArray.Owned.extract(v))
        }
      }

      hd.owned().concat(body)
    }
  }

  case class RdispatchOk(
      tag: Int,
      contexts: Seq[(Buf, Buf)],
      reply: Buf)
    extends Rdispatch(ReplyStatus.Ok, contexts, reply)

  case class RdispatchError(
      tag: Int,
      contexts: Seq[(Buf, Buf)],
      error: String)
    extends Rdispatch(ReplyStatus.Error, contexts, encodeString(error))

  case class RdispatchNack(
      tag: Int,
      contexts: Seq[(Buf, Buf)])
    extends Rdispatch(ReplyStatus.Nack, contexts, Buf.Empty)

  /**
   * A fragment, as defined by the mux spec, is a message with its tag MSB
   * set to 1.
   */
  case class Fragment(typ: Byte, tag: Int, buf: Buf) extends Message

  /** Indicates to the client to stop sending new requests. */
  case class Tdrain(tag: Int) extends EmptyMessage { def typ = Types.Tdrain }

  /** Response from the client to a `Tdrain` message */
  case class Rdrain(tag: Int) extends EmptyMessage { def typ = Types.Rdrain }

  /** Used to check liveness */
  case class Tping(tag: Int) extends EmptyMessage { def typ = Types.Tping }

  /**
   * We pre-encode a ping message with the reserved ping tag
   * (PingTag) in order to avoid re-encoding this frequently sent
   * message.
   */
  final class PreEncodedTping extends Message {
    def typ = ???
    def tag = ???
    lazy val buf = encode(Tping(Tags.PingTag))
  }

  /** Response to a `Tping` message */
  case class Rping(tag: Int) extends EmptyMessage { def typ = Types.Rping }

  /** Indicates that the corresponding T message produced an error. */
  case class Rerr(tag: Int, error: String) extends Message {
    // Use the old Rerr type in a transition period so that we
    // can be reasonably sure we remain backwards compatible with
    // old servers.

    def typ = Types.BAD_Rerr
    lazy val buf: Buf = encodeString(error)
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
    lazy val buf: Buf = {
      val arr = Array[Byte](
        (which >> 16 & 0xff).toByte,
        (which >> 8 & 0xff).toByte,
        (which & 0xff).toByte)
      Buf.ByteArray.Owned(arr).concat(encodeString(why))
    }
  }

  case class Rdiscarded(tag: Int) extends Message {
    def typ = Types.Rdiscarded
    def buf: Buf = Buf.Empty
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
    lazy val buf: Buf = {
      val bw = ByteWriter.fixed(9)
      bw.writeByte(unit)
      bw.writeLongBE(howLong)
      bw.owned()
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

  def decodeUtf8(buf: Buf): String = buf match {
    case Buf.Utf8(str) => str
    case _ => throwBadMessageException(s"expected Utf8 string, but got $buf")
  }

  def encodeString(str: String): Buf = Buf.Utf8(str)

  private def decodeTreq(tag: Int, br: ByteReader):Treq = {
    if (br.remaining < 1)
      throwBadMessageException("short Treq")

    var nkeys = br.readByte().toInt
    if (nkeys < 0)
      throwBadMessageException("Treq: too many keys")

    var trace3: Option[(SpanId, SpanId, SpanId)] = None
    var traceFlags = 0L

    while (nkeys > 0) {
      if (br.remaining < 2)
        throwBadMessageException("short Treq (header)")

      val key = br.readByte()
      val vsize = br.readByte().toInt match {
        case s if s < 0 => s + 256
        case s => s
      }

      if (br.remaining < vsize)
        throwBadMessageException("short Treq (vsize)")

      // TODO: technically we should probably check for duplicate
      // keys, but for now, just pick the latest one.
      key match {
        case Treq.Keys.TraceId =>
          if (vsize != 24)
            throwBadMessageException(s"bad traceid size $vsize")
          trace3 = Some((
            SpanId(br.readLongBE()),  // spanId
            SpanId(br.readLongBE()),  // parentId
            SpanId(br.readLongBE()))  // traceId
          )

        case Treq.Keys.TraceFlag =>
          // We only know about bit=0, so discard
          // everything but the last byte
          if (vsize > 1)
            br.readBytes(vsize-1)
          if (vsize > 0)
            traceFlags = br.readByte().toLong

        case _ =>
          // discard:
          br.readBytes(vsize)
      }

      nkeys -= 1
    }

    val id = trace3 match {
      case Some((spanId, parentId, traceId)) =>
        Some(TraceId(Some(traceId), Some(parentId), spanId, None, Flags(traceFlags)))
      case None => None
    }

    Treq(tag, id, br.readAll())
  }

  private def decodeContexts(br: ByteReader): Seq[(Buf, Buf)] = {
    val n = br.readShortBE()
    if (n == 0)
      return Nil

    val contexts = new Array[(Buf, Buf)](n)
    var i = 0
    while (i < n) {
      val k = br.readBytes(br.readShortBE())
      val v = br.readBytes(br.readShortBE())

      // For the context keys and values we want to decouple the backing array from the rest of
      // the Buf that composed the message. The body of the message is typically on the order of a
      // number of kilobytes and can be much larger, while the context entries are typically
      // on the order of < 100 bytes. Therefore, it is beneficial to decouple the lifetimes
      // at the cost of copying the context entries, but this ends up being a net win for the GC.
      // More context can be found in ``RB_ID=559066``.
      contexts(i) = (coerceTrimmed(k), coerceTrimmed(v))
      i += 1
    }
    contexts
  }

  private def decodeTdispatch(tag: Int, br: ByteReader) = {
    val contexts = decodeContexts(br)
    val ndst = br.readShortBE()
    // Path.read("") fails, so special case empty-dst.
    val dst =
      if (ndst == 0) Path.empty
      else Path.read(decodeUtf8(br.readBytes(ndst)))

    val nd = br.readShortBE()
    val dtab = if (nd == 0) Dtab.empty else {
      var i = 0
      val delegations = new Array[Dentry](nd)
      while (i < nd) {
        val src = decodeUtf8(br.readBytes(br.readShortBE()))
        val dst = decodeUtf8(br.readBytes(br.readShortBE()))
        delegations(i) = Dentry(Path.read(src), NameTree.read(dst))
        i += 1
      }
      Dtab(delegations)
    }

    Tdispatch(tag, contexts, dst, dtab, br.readAll())
  }

  private def decodeRdispatch(tag: Int, br: ByteReader) = {
    val status = br.readByte()
    val contexts = decodeContexts(br)
    val rest = br.readAll()
    status match {
      case ReplyStatus.Ok => RdispatchOk(tag, contexts, rest)
      case ReplyStatus.Error => RdispatchError(tag, contexts, decodeUtf8(rest))
      case ReplyStatus.Nack => RdispatchNack(tag, contexts)
      case _ => throwBadMessageException("invalid Rdispatch status")
    }
  }

  private def decodeRreq(tag: Int, br: ByteReader) = {
    if (br.remaining < 1)
    throwBadMessageException("short Rreq")
    val status = br.readByte()
    val rest = br.readAll()
    status match {
      case ReplyStatus.Ok => RreqOk(tag, rest)
      case ReplyStatus.Error => RreqError(tag, decodeUtf8(rest))
      case ReplyStatus.Nack => RreqNack(tag)
      case _ => throwBadMessageException("invalid Rreq status")
    }
  }

  private def decodeTdiscarded(br: ByteReader) = {
    if (br.remaining < 3)
      throwBadMessageException("short Tdiscarded message")
    val which = ((br.readByte() & 0xff) << 16) |
      ((br.readByte() & 0xff) << 8) |
      (br.readByte() & 0xff)
    Tdiscarded(which, decodeUtf8(br.readAll()))
  }

  private def decodeTlease(br: ByteReader) = {
    if (br.remaining < 9)
      throwBadMessageException("short Tlease message")
    val unit: Byte = br.readByte()
    val howMuch: Long = br.readLongBE()
    Tlease(unit, howMuch)
  }


  /**
   * Try to decode a `buf` to [[Message]]. If [[Buf]] is backed
   * by a direct buffer then that buffer will be released after decode.
   *
   * @note may throw a [[Failure]] wrapped [[BadMessageException]]
   */
  def decode(buf: Buf): Message = {
    try decode(ByteReader(buf))
    finally Bufs.releaseDirect(buf)
  }

  /**
   * Try to decode the contents of a `ByteReader` to [[Message]]. This function
   * assumes the content of the `ByteReader` represents exactly one message.
   *
   * @note may throw a [[Failure]] wrapped [[BadMessageException]]
   */
  def decode(byteReader: ByteReader): Message = {
    if (byteReader.remaining < 4) throwBadMessageException(s"short message: ${Buf.slowHexString(byteReader.readAll())}")
    val head = byteReader.readIntBE()
    val typ = Tags.extractType(head)
    val tag = Tags.extractTag(head)

    if (Tags.isFragment(tag)) Fragment(typ, tag, byteReader.readAll())
    else typ match {
      case Types.Tinit =>
        val (version, ctx) = Init.decode(byteReader)
        Tinit(tag, version, ctx)
      case Types.Rinit =>
        val (version, ctx) = Init.decode(byteReader)
        Rinit(tag, version, ctx)
      case Types.Treq => decodeTreq(tag, byteReader)
      case Types.Rreq => decodeRreq(tag, byteReader)
      case Types.Tdispatch => decodeTdispatch(tag, byteReader)
      case Types.Rdispatch => decodeRdispatch(tag, byteReader)
      case Types.Tdrain => Tdrain(tag)
      case Types.Rdrain => Rdrain(tag)
      case Types.Tping => Tping(tag)
      case Types.Rping => Rping(tag)
      case Types.Rerr | Types.BAD_Rerr => Rerr(tag, decodeUtf8(byteReader.readAll()))
      case Types.Rdiscarded => Rdiscarded(tag)
      case Types.Tdiscarded | Types.BAD_Tdiscarded => decodeTdiscarded(byteReader)
      case Types.Tlease => decodeTlease(byteReader)
      case unknown => throwBadMessageException(unknownMessageDescription(unknown, tag, byteReader))
    }
  }

  def encode(msg: Message): Buf = msg match {
    case m: PreEncodedTping => m.buf
    case m: Message =>
      if (m.tag < Tags.MarkerTag || (m.tag & ~Tags.TagMSB) > Tags.MaxTag)
        throwBadMessageException(s"invalid tag number ${m.tag}")

      val head = Array[Byte](
        m.typ,
        (m.tag >> 16 & 0xff).toByte,
        (m.tag >> 8 & 0xff).toByte,
        (m.tag & 0xff).toByte
      )

      Buf.ByteArray.Owned(head).concat(m.buf)
  }

  // Helper method to ensure conformity of BadMessageExceptions
  private def throwBadMessageException(why: String): Nothing =
    throw Failure.wrap(BadMessageException(why))

  private def unknownMessageDescription(tpe: Byte, tag: Int, payload: ByteReader): String = {
    val remaining = payload.remaining
    val toWrite = payload.readBytes(16) // Limit reporting to at most 16 bytes
    val bytesStr = Buf.slowHexString(toWrite)
    s"unknown message type: $tpe [tag=$tag]. Payload bytes: $remaining. " +
      s"First ${toWrite.length} bytes of the payload: '$bytesStr'"
  }



  /**
   * Safely coerce the Buf to a representation that doesn't hold a reference to unused data.
   *
   * The resulting Buf will be either the Empty Buf if the input Buf has zero content, or a
   * ByteArray whos underlying array contains only the bytes exposed by the Buf, making a
   * copy of the data if necessary.
   *
   * For example, calling `coerceTrimmed` on a Buf that is a 10 byte slice of a larger Buf
   * which contains 1 KB of data will yield a new ByteArray backed by a new Array[Byte]
   * containing only the 10 bytes exposed by the passed slice.
   *
   * @note exposed for testing.
   */
  private[transport] def coerceTrimmed(buf: Buf): Buf = buf match {
    case buf if buf.isEmpty => Buf.Empty
    case Buf.ByteArray.Owned(bytes, begin, end) if begin == 0 && end == bytes.length => buf
    case buf =>
      val bytes = Buf.ByteArray.Owned.extract(buf)
      Buf.ByteArray.Owned(bytes)
  }
}
