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
      .foreach(msg.params) { case((key, value), w) => w.string(key).string(value) }
      .byte(0)
  }

  implicit val passwordEncoder: MessageEncoder[FrontendMessage.PasswordMessage] = MessageEncoder('p') { (writer, msg) =>
    writer.string(msg.password)
  }

  implicit val queryEncoder: MessageEncoder[FrontendMessage.Query] = MessageEncoder('Q') { (writer, msg) =>
    writer.string(msg.value)
  }

  implicit val syncEncoder: MessageEncoder[FrontendMessage.Sync.type] = MessageEncoder('S') { (writer, msg) => writer }
}
