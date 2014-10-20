package com.twitter.finagle.thrift

import com.twitter.finagle.CodecFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.test.{AnException, B, SomeStruct}
import com.twitter.util.{Await, Future, Promise, RandomSocket, Return}
import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.CyclicBarrier
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.transport.{TFramedTransport, TServerSocket, TTransportFactory}
import org.junit.runner.RunWith
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FinagleClientThriftServerTest extends FunSuite with OneInstancePerTest {
  /* The entire block is marked as flaky because both tests are flaky.
   * If only one test is fixed, and is longer flaky, then this guard
   * can be moved back down to the individual test.
   */

  trait TestServer {
    def server: InetSocketAddress
    def shutdown: Unit
  }

  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) {
      var somewayPromise = new Promise[Unit]
      def makeServer(transportFactory: TTransportFactory)(f: (Int, Int) => Int) = {
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
          val serverAddr = RandomSocket()
          val socket = new ServerSocket(serverAddr.getPort)
          socket.setReuseAddress(true)
          val serverSocketTransport = new TServerSocket(socket)

          val server = new TSimpleServer(
            new B.Processor(processor),
            serverSocketTransport,
            transportFactory,
            new TBinaryProtocol.Factory()
          )

          (serverAddr, server)
        }

        val thriftServerThread = new Thread("thriftServer") {
          override def run() = thriftServer.serve()
        }
        thriftServerThread.start()

        new TestServer {
          def shutdown: Unit = thriftServer.stop()

          def server: InetSocketAddress = thriftServerAddr
        }
      }

      def doit(
        transportFactory: TTransportFactory,
        codec: CodecFactory[ThriftClientRequest, Array[Byte]]#Client,
        named: String
      ) {
        test("%s:finagle client vs. synchronous thrift server should talk to each other".format(named)) {
          // TODO: interleave requests (to test seqids, etc.)

          val testServer = makeServer(transportFactory) { (a, b) => a + b }

          // ** Set up the client & query the server.
          val service = ClientBuilder()
            .hosts(Seq(testServer.server))
            .codec(codec)
            .hostConnectionLimit(1)
            .build()

          val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

          val future = client.multiply(1, 2)
          assert(Await.result(future) === 3)
          testServer.shutdown
        }

        test("%s:finagle client vs. synchronous thrift server should handle exceptions".format(named)) {
          val testServer = makeServer(transportFactory) { (a, b) => a + b }

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
          val testServer = makeServer(transportFactory) { (a, b) => a + b }

          // ** Set up the client & query the server.
          val service = ClientBuilder()
            .hosts(Seq(testServer.server))
            .codec(codec)
            .hostConnectionLimit(1)
            .build()

          val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

          Await.result(client.add_one(1, 2))
          assert(true === true)
          testServer.shutdown
        }

        // race condition..
        test("%s:finagle client vs. synchronous thrift server should handle one-way calls".format(named)) {
          val testServer = makeServer(transportFactory) { (a, b) => a + b }

          // ** Set up the client & query the server.
          val service = ClientBuilder()
            .hosts(Seq(testServer.server))
            .codec(codec)
            .hostConnectionLimit(1)
            .build()

          val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

          assert(somewayPromise.isDefined === false)
          assert(Await.result(client.someway()) === null)  // returns
          assert(Await.result(somewayPromise) === (()))

          testServer.shutdown
        }

        test("%s:finagle client vs. synchronous thrift server should talk to multiple servers".format(named)) {
          val NumParties = 10
          val barrier = new CyclicBarrier(NumParties)

          val addrs = 0 until NumParties map { _ =>
            makeServer(transportFactory) { (a, b) => barrier.await(); a + b }
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
            val resolved = futures map(Await.result(_))
            resolved foreach { r => assert(r === (3)) }
          }

          addrs.foreach(_.shutdown)
        }
      }

      // Flaky test
      doit(new TFramedTransport.Factory(), ThriftClientFramedCodec(), "framed transport")

      // Flaky test
      doit(new TTransportFactory, ThriftClientBufferedCodec(), "buffered transport")
  }
}
