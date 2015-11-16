package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.test._
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Future, Promise, Return, Time}
import java.net.{InetAddress, InetSocketAddress}
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThriftClientFinagleServerTest extends FunSuite with BeforeAndAfter with OneInstancePerTest {

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

    def show_me_your_dtab() = Future.value("")
    def show_me_your_dtab_size() = Future.value(0)
  }

  val server = ServerBuilder()
    .codec(ThriftServerFramedCodec())
    .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    .name("ThriftServer")
    .build(new B.Service(processor, new TBinaryProtocol.Factory()))
  val serverAddr = server.boundAddress.asInstanceOf[InetSocketAddress]

  val (client, transport) = {
    val socket = new TSocket(serverAddr.getHostName, serverAddr.getPort, 1000/*ms*/)
    val transport = new TFramedTransport(socket)
    val protocol = new TBinaryProtocol(transport)
    (new B.Client(protocol), transport)
  }
  transport.open()

  after {
    server.close(20.milliseconds)
  }

  test("thrift client with finagle server should make successful (void) RPCs") {
    client.add_one(1, 2)
  }

  test("thrift client with finagle server should propagate exceptions") {
    val exc = intercept[AnException] { client.add(1, 2) }
    assert(exc != null)
  }

  test("thrift client with finagle server should handle complex return values") {
    assert(client.complex_return("a string").arg_two == "a string")
  }

  test("treat undeclared exceptions as internal failures") {
    val exc = intercept[TApplicationException] { client.multiply(1, 0/*div by zero*/) }
    assert(exc.getMessage() == "Internal error processing multiply: 'java.lang.ArithmeticException: / by zero'")
  }

  test("treat synchronous exceptions as transport exceptions") {
    val exc = intercept[TApplicationException] { client.complex_return("throwAnException") }
    assert(exc.getMessage() == "Internal error processing complex_return: 'java.lang.Exception: msg'")
  }

  test("handle one-way calls") {
    assert(somewayPromise.isDefined == false)
    client.someway()                  // just returns(!)
    assert(Await.result(somewayPromise) == ())
  }

  test("handle wrong interface") {
    val (client, transport) = {
      val socket = new TSocket(serverAddr.getHostName, serverAddr.getPort, 1000/*ms*/)
      val transport = new TFramedTransport(socket)
      val protocol = new TBinaryProtocol(transport)
      (new F.Client(protocol), transport)
    }
    transport.open()

    val exc = intercept[TApplicationException] { client.another_method(123) }
    assert(exc.getMessage() == "Invalid method name: 'another_method'")
  }

  test("make sure we measure protocol negotiation latency") {
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
      assert(Await.result(client.multiply(4,2)) == 2)

      val key = Seq(name, "codec_connection_preparation_latency_ms")
      assert(statsReceiver.repr.stats.contains(key) == true)
    }
  }
}
