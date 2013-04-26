package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.test._
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Future, Promise, Return, Time}
import java.net.InetSocketAddress
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.specs.SpecificationWithJUnit

class ThriftClientFinagleServerSpec extends SpecificationWithJUnit {
  "thrift client with finagle server" should {
    val somewayPromise = new Promise[Unit]
    val processor = new B.ServiceIface {
      def add(a: Int, b: Int) = Future.exception(new AnException)
      def add_one(a: Int, b: Int) = Future.Void
      def multiply(a: Int, b: Int) = Future { a / b }
      def complex_return(someString: String) =
        someString match {
          case "throwAnException" =>
            throw new Exception("msg")
          case _ =>
            Future { new SomeStruct(123, someString) }
        }
      def someway() = {
        somewayPromise() = Return(())
        Future.Void
      }
    }

    val server = ServerBuilder()
      .codec(ThriftServerFramedCodec())
      .bindTo(new InetSocketAddress(0))
      .name("ThriftServer")
      .build(new B.Service(processor, new TBinaryProtocol.Factory()))
    val serverAddr = server.localAddress.asInstanceOf[InetSocketAddress]

    doAfter {
      server.close(20.milliseconds)
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
        new TApplicationException("Internal error processing multiply: 'java.lang.ArithmeticException: / by zero'"))
    }

    "treat synchronous exceptions as transport exceptions" in {
      client.complex_return("throwAnException") must throwA(
        new TApplicationException("Internal error processing complex_return: 'java.lang.Exception: msg'"))
    }

    "handle one-way calls" in {
      somewayPromise.isDefined must beFalse
      client.someway()                  // just returns(!)
      Await.result(somewayPromise) must be_==(())
    }

    "handle wrong interface" in {
      val (client, transport) = {
        val socket = new TSocket(serverAddr.getHostName, serverAddr.getPort, 1000/*ms*/)
        val transport = new TFramedTransport(socket)
        val protocol = new TBinaryProtocol(transport)
        (new F.Client(protocol), transport)
      }
      transport.open()

      client.another_method(123) must throwA(
        new TApplicationException("Invalid method name: 'another_method'"))
    }

    "make sure we measure protocol negotiation latency" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val statsReceiver = new InMemoryStatsReceiver
        val name = "thrift_client"
        val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
          .hosts(serverAddr)
          .name(name)
          .hostConnectionLimit(1)
          .codec(ThriftClientFramedCodec())
          .reportTo(statsReceiver)
          .build()

        val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())
        Await.result(client.multiply(4,2)) must be_==(2)

        val key = Seq(name, "codec_connection_preparation_latency_ms")
        statsReceiver.repr.stats.contains(key) must beTrue
      }
    }
  }
}
