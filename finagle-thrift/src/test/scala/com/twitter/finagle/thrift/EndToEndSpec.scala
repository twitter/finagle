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
        def add(a: Int, b: Int) = Future.exception(new AnException)
        def add_one(a: Int, b: Int) = Future.void
        def multiply(a: Int, b: Int) = Future { a * b }
        def complex_return(someString: String) = Future {
          new SomeStruct(123, someString)
        }
        def someway() = Future.void
      }

      val serverAddr = RandomSocket()
      val server = ServerBuilder()
        .codec(ThriftServerFramedCodec())
        .bindTo(serverAddr)
        .build(new B.Service(processor, new TBinaryProtocol.Factory()))

      val service = ClientBuilder()
        .hosts(Seq(serverAddr))
        .codec(ThriftClientFramedCodec())
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      val future = client.multiply(10, 30)
      future() must be_==(300)

      client.complex_return("a string")().arg_two must be_==("a string")

      client.add(1, 2)() must throwA[AnException]
      client.add_one(1, 2)()  // don't block! 

      client.someway()() must beNull  // don't block!

      server.close()()
    }
  }
}
