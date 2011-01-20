package com.twitter.finagle.thrift

import org.specs.Specification

import org.apache.thrift.protocol.TBinaryProtocol

import com.twitter.util.{Future, RandomSocket, Throw, Return}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}

import com.twitter.test.{B, SomeStruct, AnException}


object EndToEndSpec extends Specification {
  "Thrift server" should {
    "work end-to-end" in {
      val processor =  new B.ServiceIface {
        def add(a: Int, b: Int) = Future { a + b }
        def add_one(a: Int, b: Int) = Future.exception(new AnException)
        def multiply(a: Int, b: Int) = Future { a * b }
        def complex_return(someString: String) = Future {
          new SomeStruct(123, someString)
        }
      }

      val serverAddr = RandomSocket()
      val channel = ServerBuilder()
        .codec(ThriftFramedTransportCodec())
        .bindTo(serverAddr)
        .service(new B.Service(processor, new TBinaryProtocol.Factory()))
        .build()

      val service = ClientBuilder()
        .hosts(Seq(serverAddr))
        .codec(ThriftFramedTransportCodec())
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      val future = client.multiply(10, 30)
      future() must be_==(300)

      client.complex_return("a string")().arg_two must be_==("a string")

      client.add_one(1, 2)() must throwA[AnException]

      channel.close().awaitUninterruptibly()
    }
  }
}
