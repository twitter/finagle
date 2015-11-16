package com.twitter.finagle.thrift

import com.twitter.conversions.time._
import com.twitter.finagle.CodecFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.loadbalancer.defaultBalancer
import com.twitter.test.{AnException, B, SomeStruct}
import com.twitter.util.{Await, Promise, Return}
import java.net.{ServerSocket, SocketAddress, InetAddress}
import java.util.concurrent.CyclicBarrier
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.transport.{TFramedTransport, TServerSocket, TTransportFactory}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FinagleClientThriftServerTest extends FunSuite {
  trait TestServer {
    def server: SocketAddress
    def shutdown: Unit
  }

  def makeServer(transportFactory: TTransportFactory, somewayPromise: Promise[Unit])(f: (Int, Int) => Int) = {
    val processor = new B.Iface {
      def multiply(a: Int, b: Int): Int = f(a, b)
      def add(a: Int, b: Int): Int = { throw new AnException }
      def add_one(a: Int, b: Int) = {}
      def complex_return(someString: String) = new SomeStruct(123, someString)
      def someway() {
        somewayPromise() = Return(())
      }
      def show_me_your_dtab() = ""
      def show_me_your_dtab_size() = 0
    }

    val (thriftServerAddr, thriftServer) = {
      val loopback = InetAddress.getLoopbackAddress
      val socket = new ServerSocket(0, 50, loopback)
      val serverSocketTransport = new TServerSocket(socket)

      val server = new TSimpleServer(
        new B.Processor(processor),
        serverSocketTransport,
        transportFactory,
        new TBinaryProtocol.Factory()
      )

      (socket.getLocalSocketAddress, server)
    }

    val thriftServerThread = new Thread("thriftServer") {
      override def run() = thriftServer.serve()
    }
    thriftServerThread.start()

    new TestServer {
      def shutdown: Unit = thriftServer.stop()

      def server: SocketAddress = thriftServerAddr
    }
  }

  def doit(
    transportFactory: TTransportFactory,
    codec: CodecFactory[ThriftClientRequest, Array[Byte]]#Client,
    named: String
  ) {
    test("%s:finagle client vs. synchronous thrift server should talk to each other".format(named)) {
      val somewayPromise = new Promise[Unit]

      // TODO: interleave requests (to test seqids, etc.)

      val testServer = makeServer(transportFactory, somewayPromise) { (a, b) => a + b }

      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(Seq(testServer.server))
        .codec(codec)
        .hostConnectionLimit(1)
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      val future = client.multiply(1, 2)
      assert(Await.result(future) == 3)
      testServer.shutdown
    }

    test("%s:finagle client vs. synchronous thrift server should handle exceptions".format(named)) {
      val somewayPromise = new Promise[Unit]

      val testServer = makeServer(transportFactory, somewayPromise) { (a, b) => a + b }

      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(Seq(testServer.server))
        .codec(codec)
        .hostConnectionLimit(1)
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      intercept[Exception]{
        Await.result(client.add(1, 2))
      }
      testServer.shutdown
    }

    test("%s:finagle client vs. synchronous thrift server should handle void returns".format(named)) {
      val somewayPromise = new Promise[Unit]
      val testServer = makeServer(transportFactory, somewayPromise) { (a, b) => a + b }

      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(Seq(testServer.server))
        .codec(codec)
        .hostConnectionLimit(1)
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      Await.result(client.add_one(1, 2))
      assert(true == true)
      testServer.shutdown
    }

    // race condition..
    test("%s:finagle client vs. synchronous thrift server should handle one-way calls".format(named)) {
      val somewayPromise = new Promise[Unit]
      val testServer = makeServer(transportFactory, somewayPromise) { (a, b) => a + b }

      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(Seq(testServer.server))
        .codec(codec)
        .hostConnectionLimit(1)
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      assert(somewayPromise.isDefined == false)
      assert(Await.result(client.someway()) == null)  // returns
      assert(Await.result(somewayPromise) == (()))

      testServer.shutdown
    }

    // this test assumes that N requests will be evenly distributed to N hosts.
    if (defaultBalancer() == "heap")
    test(s"$named:finagle client vs. synchronous thrift server should talk to multiple servers") {
      val somewayPromise = new Promise[Unit]
      val NumParties = 10
      val barrier = new CyclicBarrier(NumParties)

      val addrs = 0 until NumParties map { _ =>
        makeServer(transportFactory, somewayPromise) { (a, b) => barrier.await(); a + b }
      }

      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(addrs.map(_.server))
        .codec(codec)
        .hostConnectionLimit(1)
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      {
        val futures = 0 until NumParties map { _ => client.multiply(1, 2) }
        val resolved = futures map (Await.result(_, 5.seconds))
        resolved foreach { r => assert(r == (3)) }
      }

      addrs.foreach(_.shutdown)
    }
  }

  doit(new TFramedTransport.Factory(), ThriftClientFramedCodec(), "framed transport")

  doit(new TTransportFactory, ThriftClientBufferedCodec(), "buffered transport")
}
