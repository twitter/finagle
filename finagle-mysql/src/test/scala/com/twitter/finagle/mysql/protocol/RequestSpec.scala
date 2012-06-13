package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit

class RequestSpec extends SpecificationWithJUnit {

  "Command Request" should {
    "encode" in {
      val args = "dbname"
      val data = new CommandRequest(Request.COM_INIT_DB, args.getBytes).encoded
      val size = Packet.headerSize + 1 + args.length
      size mustEqual data.size
      data(Packet.headerSize) mustEqual Request.COM_INIT_DB
      data.drop(Packet.headerSize+1).take(args.size) must containAll(args.getBytes)
    }
  }
}