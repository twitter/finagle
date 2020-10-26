package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.FrontendMessage

/**
 * A typeclass for encoding [[FrontendMessage]] to a [[Packet]].
 *
 * @see [[MessageDecoder]]
 * @see [[PgBuf.Writer]]
 */
trait MessageEncoder[M <: FrontendMessage] {
  def toPacket(m: M): Packet
}

object MessageEncoder {

  def apply[M <: FrontendMessage](b: Option[Byte])(f: (PgBuf.Writer, M) => PgBuf.Writer): MessageEncoder[M] =
    m => Packet(b, f(PgBuf.writer, m).build)

  def apply[M <: FrontendMessage](c: Char)(f: (PgBuf.Writer, M) => PgBuf.Writer): MessageEncoder[M] =
    apply(Some(c.toByte))(f)

  def emptyMessageEncoder[M <: FrontendMessage](b: Byte): MessageEncoder[M] = MessageEncoder(Some(b)) { (writer, _) =>
    writer
  }

  implicit val sslRequestEncoder: MessageEncoder[FrontendMessage.SslRequest.type] = MessageEncoder(None) {
    (writer, _) =>
      // "The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits, and 5679 in the least significant 16 bits.
      // (To avoid confusion, this code must not be the same as any protocol version number.)"
      writer.int(80877103)
  }

  implicit val startupEncoder: MessageEncoder[FrontendMessage.StartupMessage] = MessageEncoder(None) { (writer, msg) =>
    writer
      .short(msg.version.major)
      .short(msg.version.minor)
      .cstring("user").cstring(msg.user)
      .opt(msg.database) { (w, db) =>
        w.cstring("database").cstring(db)
      }
      .foreachUnframed(msg.params) { case (w, (key, value)) => w.cstring(key).cstring(value) }
      .byte(0)
  }

  implicit val passwordEncoder: MessageEncoder[FrontendMessage.PasswordMessage] = MessageEncoder('p') { (writer, msg) =>
    writer.cstring(msg.password)
  }

  implicit val queryEncoder: MessageEncoder[FrontendMessage.Query] = MessageEncoder('Q') { (writer, msg) =>
    writer.cstring(msg.value)
  }

  implicit val syncEncoder: MessageEncoder[FrontendMessage.Sync.type] = emptyMessageEncoder('S')
  implicit val flushEncoder: MessageEncoder[FrontendMessage.Flush.type] = emptyMessageEncoder('H')

  implicit val parseEncoder: MessageEncoder[FrontendMessage.Parse] = MessageEncoder('P') { (writer, msg) =>
    writer
      .name(msg.name)
      .cstring(msg.statement)
      .foreach(msg.dataTypes) { (w, oid) =>
        w.unsignedInt(oid.value)
      }
  }

  implicit val bindEncoder: MessageEncoder[FrontendMessage.Bind] = MessageEncoder('B') { (writer, msg) =>
    writer
      .name(msg.portal)
      .name(msg.statement)
      .foreach(msg.formats)((w, f) => w.format(f))
      .foreach(msg.values)((w, v) => w.value(v))
      .foreach(msg.resultFormats)((w, f) => w.format(f))
  }

  implicit val describeEncoder: MessageEncoder[FrontendMessage.Describe] = MessageEncoder('D') { (writer, msg) =>
    writer
      .byte(msg.target match {
        case FrontendMessage.DescriptionTarget.Portal => 'P'
        case FrontendMessage.DescriptionTarget.PreparedStatement => 'S'
      })
      .name(msg.name)
  }

  implicit val executeEncoder: MessageEncoder[FrontendMessage.Execute] = MessageEncoder('E') { (writer, msg) =>
    writer
      .name(msg.portal)
      .int(msg.maxRows)
  }
}
