package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.mysql.util.BufferUtil

class PacketSpec extends SpecificationWithJUnit {
  "Packet" should {
    val size = 5
    val seq = 0.toShort
    val header = PacketHeader(size, seq)
    
    "Encode Header" in {
     val br = BufferReader(header.toChannelBuffer)
     size mustEqual br.readInt24()
     seq mustEqual br.readByte()
    }
  }
}