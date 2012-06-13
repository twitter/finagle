package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql.util.ByteArrayUtil

object Packet {
  val headerSize = 0x04

  def apply(packetSize: Int, seq: Byte) = new Packet {
    val size = packetSize
    val number = seq
    val body = new Array[Byte](size)
  }

  def apply(packetSize: Int, seq: Byte, data: Array[Byte]) = new Packet {
    val size = packetSize
    val number = seq
    val body = data
  }
}

trait Packet {
  val size: Int
  val number: Byte //used for sanity checks on server side
  val body: Array[Byte]

  lazy val header: Array[Byte] = {
    val hdr = new Array[Byte](Packet.headerSize)
    ByteArrayUtil.write(size, hdr, 0, 3)
    hdr(3) = number
    hdr
  }
}


/**
 * TODO: Implement packet compression with zlib. We 
 * can use the Inflater and Deflater in java.util.zip.
 *
 * Note: Using compression might not always be advantageous.
 * It is possible for the body (data) of the packet to remain
 * uncompressed if calling compress() does not yield a smaller
 * body. This happens in a session with small packets or
 * poorly compressable packets. (MySQL Internals, pg. 63)
 */
//class CompressedPacket extends Packet 