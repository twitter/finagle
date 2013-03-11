package com.twitter.finagle.exp.mysql.protocol

import org.specs.SpecificationWithJUnit

class ServersGreetingSpec extends SpecificationWithJUnit {
  "ServersGreeting" should {
    val data = Array[Byte](
      10,53,46,53,46,50,52,0,31,0,0,0,70,38,43,66,74,
      48,79,126,0,-1,-9,33,2,0,15,-128,21,0,0,0,0,0,
      0,0,0,0,0,76,66,70,118,67,40,63,68,120,80,103,54,0
    )

    val p = Packet(data.length, 0, data)

    "decode" in {
      val sg = ServersGreeting.decode(p)

      "protocol number" in {
        sg.protocol mustEqual 10
      }

      "version" in {
        sg.version mustEqual "5.5.24"
      }

      "thread id" in {
        sg.threadId mustEqual 31
      }

      "salt" in {
        sg.salt.length mustEqual 20
        sg.salt must containAll(Array[Byte](70,38,43,66,74,48,79,126,76,66,
                              70,118,67,40,63,68,120,80,103,54))
      }

      "server capabilities" in {
        sg.serverCap.mask mustEqual 0xf7ff
      }

      "server collation code" in {
        sg.charset mustEqual 33
      }

      "server status" in {
        sg.status mustEqual 2
      }
    }

  }
}
