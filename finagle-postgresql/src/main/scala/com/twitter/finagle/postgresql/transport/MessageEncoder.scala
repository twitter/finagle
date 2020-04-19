package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.FrontendMessage

trait MessageEncoder[M <: FrontendMessage] {
  def toPacket(m: M): Packet
}

object MessageEncoder {

  def apply[M <: FrontendMessage](b: Option[Byte])(f: (PgBuf.Writer, M) => PgBuf.Writer): MessageEncoder[M] =
    m => Packet(b, f(PgBuf.writer, m).build)

  def apply[M <: FrontendMessage](c: Char)(f: (PgBuf.Writer, M) => PgBuf.Writer): MessageEncoder[M] =
    apply(Some(c.toByte))(f)

  def emptyMessageEncoder[M <: FrontendMessage](b: Byte): MessageEncoder[M] = MessageEncoder(Some(b)) { (writer, _) => writer }

  implicit val sslRequestEncoder: MessageEncoder[FrontendMessage.SslRequest.type] = MessageEncoder(None) { (writer, _) =>
    writer.int(80877103)
  }

  implicit val startupEncoder: MessageEncoder[FrontendMessage.StartupMessage]  = MessageEncoder(None) { (writer, msg) =>
    writer
      .short(msg.version.major)
      .short(msg.version.minor)
      .string("user").string(msg.user)
      .opt(msg.database) { (db, w) =>
        w.string("database").string(db)
      }
      .foreachUnframed(msg.params) { case((key, value), w) => w.string(key).string(value) }
      .byte(0)
  }

  implicit val passwordEncoder: MessageEncoder[FrontendMessage.PasswordMessage] = MessageEncoder('p') { (writer, msg) =>
    writer.string(msg.password)
  }

  implicit val queryEncoder: MessageEncoder[FrontendMessage.Query] = MessageEncoder('Q') { (writer, msg) =>
    writer.string(msg.value)
  }

  implicit val syncEncoder: MessageEncoder[FrontendMessage.Sync.type] = emptyMessageEncoder('S')
  implicit val flushEncoder: MessageEncoder[FrontendMessage.Flush.type] = emptyMessageEncoder('H')

  implicit val parseEncoder: MessageEncoder[FrontendMessage.Parse] = MessageEncoder('P') { (writer, msg) =>
    writer
      .name(msg.name)
      .string(msg.statement)
      .foreach(msg.dataTypes) { (oid, w) =>
        w.int(oid.value)
      }
  }

  implicit val bindEncoder: MessageEncoder[FrontendMessage.Bind] = MessageEncoder('B') { (writer, msg) =>
    writer
      .name(msg.portal)
      .name(msg.statement)
      .foreach(msg.formats) { (f, w) => w.format(f) }
      .foreach(msg.values) { (v, w) => w.value(v) }
      .foreach(msg.resultFormats) { (f, w) => w.format(f) }
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
