package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.mysql.util.ByteArrayUtil

class PacketSpec extends SpecificationWithJUnit {
  "Packet" should {
    val size = 5
    val seq = 0.toByte
    val data = Array[Byte](116, 119, 105, 116, 116, 101, 114) 
    val p = Packet(size, seq, data)
    
    "Contain correct header size" in {
      val n = ByteArrayUtil.read(p.header, 0, 3)
      n mustEqual size
    }

    "Contain correct header number" in {
      val n = ByteArrayUtil.read(p.header, 3, 1)
      n mustEqual seq
    }

    "Contain correct body" in {
      p.body must containAll(data)
    }
  }
}