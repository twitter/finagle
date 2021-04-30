package com.twitter.finagle.mysql.transport

import com.twitter.io.Buf

object Packet {
  val HeaderSize = 0x04
  val OkByte = 0x00.toByte
  val ErrorByte = 0xff.toByte
  val EofByte = 0xfe.toByte
  val AuthMoreDataByte = 0x01.toByte

  val MaxBodySize = 0xffffff

  def fromBuf(buf: Buf): Packet = {
    val br = MysqlBuf.reader(buf)
    try {
      val size = br.readUnsignedMediumLE()
      val seq = br.readUnsignedByte()
      val body = br.readAll()
      if (size != body.length) {
        throw new IllegalStateException(s"Bad Packet size. Expected: $size, actual ${body.length}")
      }
      Packet(seq, body)
    } finally br.close()
  }
}

/**
 * A logical packet exchanged between the mysql
 * server and client. A packet consists of a header
 * (size, sequence number) and a body.
 */
case class Packet(seq: Short, body: Buf) {

  /**
   * Size of packet body. Encoded in the first 3
   * bytes of the packet.
   */
  def size: Int = body.length

  def toBuf: Buf = {
    val bw = MysqlBuf.writer(new Array[Byte](Packet.HeaderSize))
    bw.writeMediumLE(size)
    bw.writeByte(seq)
    bw.owned().concat(body)
  }
}
