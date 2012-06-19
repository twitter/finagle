package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.mysql.util.BufferUtil

class PacketSpec extends SpecificationWithJUnit {
  "Packet" should {
    val size = 5
    val seq = 0.toByte
    val data = Array[Byte](116, 119, 105, 116, 116, 101, 114)
    val p = Packet(size, seq, data)
    val br = new BufferReader(p.header)
    
    "Contain correct header size" in {
      val n = br.readInt24
      n mustEqual size
    }

    "Contain correct header number" in {
      br.skip(3)
      val n = br.readByte
      n mustEqual seq
    }

    "Contain correct body" in {
      p.body must containAll(data)
    }
  }
}