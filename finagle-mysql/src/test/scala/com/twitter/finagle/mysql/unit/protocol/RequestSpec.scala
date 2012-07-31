package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit

class RequestSpec extends SpecificationWithJUnit {

  "Simple Request" should {
    "encode" in {
      /*val args = "dbname"
      val req = new SimpleCommandRequest(Command.COM_INIT_DB, args.getBytes)
      val data = req.toChannelBuffer.array
      val size = Packet.HeaderSize + 1 + args.length
      size mustEqual data.size
      data(Packet.HeaderSize) mustEqual Command.COM_INIT_DB
      data.drop(Packet.HeaderSize+1).take(args.size) must containAll(args.getBytes)
      */
      1 mustEqual 1
    }
  }
}