package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.io.Buf

/**
 * A type-class for encoding [[FrontendMessage]] to a [[Buf]].
 *
 * @see [[MessageDecoder]]
 * @see [[PgBuf.Writer]]
 */
sealed trait MessageEncoder[M <: FrontendMessage] {
  def toBuf(m: M): Buf
}

object MessageEncoder {

  def toBuf[M <: FrontendMessage](msg: M)(implicit enc: MessageEncoder[M]): Buf = enc.toBuf(msg)

  private def frameBuf(buf: Buf, frameOffset: Int, frameLen: Int): Buf = {
    val Buf.ByteArray.Owned(packetData, off, len) = Buf.ByteArray.coerce(buf)

    // write `frameLen` into the buffer at `frameOffset + off` as a big-endian int.
    packetData(off + frameOffset) = ((frameLen >> 24) & 0xff).toByte
    packetData(off + frameOffset + 1) = ((frameLen >> 16) & 0xff).toByte
    packetData(off + frameOffset + 2) = ((frameLen >> 8) & 0xff).toByte
    packetData(off + frameOffset + 3) = ((frameLen) & 0xff).toByte

    Buf.ByteArray.Owned(packetData, off, len)
  }

  def apply[M <: FrontendMessage](
    cmd: Byte
  )(
    f: (PgBuf.Writer, M) => PgBuf.Writer
  ): MessageEncoder[M] =
    new MessageEncoder[M] {
      override def toBuf(m: M): Buf = {
        val writer = PgBuf.writer
          .byte(cmd)
          .int(-1) // bytes for framing

        val buf = f(writer, m).build
        frameBuf(buf, 1, buf.length - 1)
      }
    }

  def emptyMessageEncoder[M <: FrontendMessage](b: Byte): MessageEncoder[M] = MessageEncoder(b) {
    (writer, _) =>
      writer
  }

  implicit val sslRequestEncoder: MessageEncoder[FrontendMessage.SslRequest.type] = {
    new MessageEncoder[FrontendMessage.SslRequest.type] {
      // "The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits, and 5679 in the least significant 16 bits.
      // (To avoid confusion, this code must not be the same as any protocol version number.)"
      private[this] final val buf = PgBuf.writer.int(8).int(80877103).build

      override def toBuf(m: FrontendMessage.SslRequest.type): Buf = buf
    }
  }

  implicit val startupEncoder: MessageEncoder[FrontendMessage.StartupMessage] = {
    new MessageEncoder[FrontendMessage.StartupMessage] {
      override def toBuf(msg: FrontendMessage.StartupMessage): Buf = {
        val buf = PgBuf.writer
          .int(-1) // bytes for framing
          .short(msg.version.major)
          .short(msg.version.minor)
          .cstring("user").cstring(msg.user)
          .opt(msg.database) { (w, db) =>
            w.cstring("database").cstring(db)
          }
          .foreachUnframed(msg.params) { case (w, (key, value)) => w.cstring(key).cstring(value) }
          .byte(0)
          .build
        frameBuf(buf, 0, buf.length)
      }
    }
  }

  implicit val passwordEncoder: MessageEncoder[FrontendMessage.PasswordMessage] =
    MessageEncoder('p') { (writer, msg) =>
      writer.cstring(msg.password)
    }

  implicit val queryEncoder: MessageEncoder[FrontendMessage.Query] = MessageEncoder('Q') {
    (writer, msg) =>
      writer.cstring(msg.value)
  }

  implicit val syncEncoder: MessageEncoder[FrontendMessage.Sync.type] = emptyMessageEncoder('S')
  implicit val flushEncoder: MessageEncoder[FrontendMessage.Flush.type] = emptyMessageEncoder('H')

  implicit val parseEncoder: MessageEncoder[FrontendMessage.Parse] = MessageEncoder('P') {
    (writer, msg) =>
      writer
        .name(msg.name)
        .cstring(msg.statement)
        .foreach(msg.dataTypes) { (w, oid) =>
          w.unsignedInt(oid.value)
        }
  }

  implicit val bindEncoder: MessageEncoder[FrontendMessage.Bind] = MessageEncoder('B') {
    (writer, msg) =>
      writer
        .name(msg.portal)
        .name(msg.statement)
        .foreach(msg.formats)((w, f) => w.format(f))
        .foreach(msg.values)((w, v) => w.value(v))
        .foreach(msg.resultFormats)((w, f) => w.format(f))
  }

  implicit val describeEncoder: MessageEncoder[FrontendMessage.Describe] = MessageEncoder('D') {
    (writer, msg) =>
      writer
        .byte(msg.target match {
          case FrontendMessage.DescriptionTarget.Portal => 'P'
          case FrontendMessage.DescriptionTarget.PreparedStatement => 'S'
        })
        .name(msg.name)
  }

  implicit val executeEncoder: MessageEncoder[FrontendMessage.Execute] = MessageEncoder('E') {
    (writer, msg) =>
      writer
        .name(msg.portal)
        .int(msg.maxRows)
  }

  implicit val closeEncoder: MessageEncoder[FrontendMessage.Close] = MessageEncoder('C') {
    (writer, msg) =>
      writer
        .byte(msg.target match {
          case FrontendMessage.DescriptionTarget.Portal => 'P'
          case FrontendMessage.DescriptionTarget.PreparedStatement => 'S'
        })
        .name(msg.name)
  }

  implicit val copyDataEncoder: MessageEncoder[FrontendMessage.CopyData] = MessageEncoder('d') {
    (writer, msg) =>
      writer.buf(msg.bytes)
  }

  implicit val copyDoneEncoder: MessageEncoder[FrontendMessage.CopyDone.type] = emptyMessageEncoder(
    'c')

  implicit val copyFailEncoder: MessageEncoder[FrontendMessage.CopyFail] = MessageEncoder('f') {
    (writer, msg) =>
      writer.cstring(msg.msg)
  }
}
