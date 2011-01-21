package com.twitter.finagle.thrift

import org.specs.Specification

import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.{TProtocol, TBinaryProtocol}
import org.apache.thrift.transport.{
  TFramedTransport, TSocket, TTransportException, TTransport}

import com.twitter.util.{Future, RandomSocket, Throw, Return}
import com.twitter.test._

import com.twitter.finagle.builder.ServerBuilder

object ThriftClientFinagleServerSpec extends Specification {
  "thrift client with finagle server" should {
    val processor = new B.ServiceIface {
      def add(a: Int, b: Int) = Future.exception(new AnException)
      def add_one(a: Int, b: Int) = Future.void
      def multiply(a: Int, b: Int) = Future { a / b }
      def complex_return(someString: String) =
        someString match {
          case "throwAnException" =>
            throw new Exception("")
          case _ =>
            Future { new SomeStruct(123, someString) }
        }
    }

    val serverAddr = RandomSocket()
    val server = ServerBuilder()
      .codec(ThriftFramedTransportCodec())
      .bindTo(serverAddr)
      .service(new B.Service(processor, new TBinaryProtocol.Factory()))
      .build()

    doAfter {
      server.close()()
    }

    val (client, transport) = {
      val socket = new TSocket(serverAddr.getHostName, serverAddr.getPort, 1000/*ms*/)
      val transport = new TFramedTransport(socket)
      val protocol = new TBinaryProtocol(transport)
      (new B.Client(protocol), transport)
    }
    transport.open()

    "make successful (void) RPCs" in { client.add_one(1, 2); true must beTrue }
    "propagate exceptions" in { client.add(1, 2) must throwA[AnException] }
    "handle complex return values" in {
      client.complex_return("a string").arg_two must be_==("a string")
    }

    "treat undeclared exceptions as internal failures" in {
      client.multiply(1, 0/*div by zero*/) must throwA(
        new TApplicationException("Internal error processing multiply"))
    }

    "treat processor exceptions as transport exceptions" in {
      client.complex_return("throwAnException") must throwA(
        new TApplicationException("Internal error processing complex_return"))
    }
  }
}
