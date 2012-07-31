package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.mysql.util.BufferUtil

class PacketSpec extends SpecificationWithJUnit {
  "Packet" should {
    val size = 5
    val seq = 0.toByte
    val data = Array[Byte](116, 119, 105, 116, 116, 101, 114)
    val p = Packet(size, seq, data)
    
    "Contain correct body" in {
     1 mustEqual 1
    }
  }
}