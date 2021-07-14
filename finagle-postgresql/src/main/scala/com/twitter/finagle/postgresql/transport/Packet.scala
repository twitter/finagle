package com.twitter.finagle.postgresql.transport

import com.twitter.io.Buf

case class Packet(cmd: Option[Byte], body: Buf) {
  def toBuf: Buf =
    PgBuf.writer
      .opt(cmd)((w, c) => w.byte(c))
      .int(body.length + 4)
      .buf(body)
      .build
}

object Packet {
  def parse(b: Buf): Packet = {
    val reader = PgBuf.reader(b)
    val cmd = reader.byte()
    val body =
      if (reader.remaining == 0) Buf.Empty
      else {
        val length = reader.int() - 4
        val body = reader.remainingBuf()
        require(length == body.length, s"Invalid or corrupted packet. Expected $length bytes, but read ${body.length}")
        body
      }
    Packet(Some(cmd), body)
  }
}
