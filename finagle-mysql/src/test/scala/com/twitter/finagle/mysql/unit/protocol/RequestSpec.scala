package com.twitter.finagle.mysql.protocol

import org.specs.SpecificationWithJUnit

class RequestSpec extends SpecificationWithJUnit {

  "Simple Request" should {
    "encode" in {
      val args = "dbname"
      val data = new SimpleRequest(Command.COM_INIT_DB, args.getBytes).toByteArray
      val size = Packet.headerSize + 1 + args.length
      size mustEqual data.size
      data(Packet.headerSize) mustEqual Command.COM_INIT_DB
      data.drop(Packet.headerSize+1).take(args.size) must containAll(args.getBytes)
    }
  }
}