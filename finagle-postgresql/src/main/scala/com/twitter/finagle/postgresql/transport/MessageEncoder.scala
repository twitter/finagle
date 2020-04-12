package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.Messages
import com.twitter.finagle.postgresql.Messages.StartupMessage

trait MessageEncoder[M <: Messages.FrontendMessage] {
  def toPacket(m: M): Packet
}

object MessageEncoder {

  def apply[M <: Messages.FrontendMessage](b: Option[Byte])(f: (PgBuf.Writer, M) => PgBuf.Writer): MessageEncoder[M] =
    m => Packet(b, f(PgBuf.writer, m).build)

  def apply[M <: Messages.FrontendMessage](c: Char)(f: (PgBuf.Writer, M) => PgBuf.Writer): MessageEncoder[M] =
    apply(Some(c.toByte))(f)

  implicit val startupEncoder: MessageEncoder[StartupMessage]  = MessageEncoder(None) { (writer, msg) =>
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

  implicit val passwordEncoder: MessageEncoder[Messages.PasswordMessage] = MessageEncoder('p') { (writer, msg) =>
    writer.string(msg.password)
  }

  implicit val syncEncoder: MessageEncoder[Messages.Sync.type] = MessageEncoder('S') { (writer, msg) => writer }
}
