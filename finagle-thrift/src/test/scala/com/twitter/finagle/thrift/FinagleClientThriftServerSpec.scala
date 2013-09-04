package com.twitter.finagle.thrift

import com.twitter.finagle.CodecFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.test.{AnException, B, SomeStruct}
import com.twitter.util.{Await, Future, Promise, RandomSocket, Return}
import java.net.ServerSocket
import java.util.concurrent.CyclicBarrier
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.transport.{TFramedTransport, TServerSocket, TTransportFactory}
import org.specs.SpecificationWithJUnit

class FinagleClientThriftServerSpec extends SpecificationWithJUnit {
  /* The entire block is marked as flaky because both tests are flaky.
   * If only one test is fixed, and is longer flaky, then this guard
   * can be moved back down to the individual test.
   */
  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) {
    "finagle client vs. synchronous thrift server" should {
      var somewayPromise = new Promise[Unit]
      def makeServer(transportFactory: TTransportFactory)(f: (Int, Int) => Int) = {
        val processor = new B.Iface {
          def multiply(a: Int, b: Int): Int = f(a, b)
          def add(a: Int, b: Int): Int = { throw new AnException }
          def add_one(a: Int, b: Int) = {}
          def complex_return(someString: String) = new SomeStruct(123, someString)
          def someway() = {
            somewayPromise() = Return(())
            Future.Void
          }
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

        doAfter {
          thriftServer.stop()
          thriftServerThread.join()
        }

        thriftServerAddr
      }


      def doit(
        transportFactory: TTransportFactory,
        codec: CodecFactory[ThriftClientRequest, Array[Byte]]#Client
      ) {
        "talk to each other" in {
          // TODO: interleave requests (to test seqids, etc.)

          val thriftServerAddr = makeServer(transportFactory) { (a, b) => a + b }

          // ** Set up the client & query the server.
          val service = ClientBuilder()
            .hosts(Seq(thriftServerAddr))
            .codec(codec)
            .hostConnectionLimit(1)
            .build()

          val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

          val future = client.multiply(1, 2)
          Await.result(future) must be_==(3)
        }

        "handle exceptions" in {
          val thriftServerAddr = makeServer(transportFactory) { (a, b) => a + b }

          // ** Set up the client & query the server.
          val service = ClientBuilder()
            .hosts(Seq(thriftServerAddr))
            .codec(codec)
            .hostConnectionLimit(1)
            .build()

          val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

          Await.result(client.add(1, 2)) must throwA[AnException]
        }

        "handle void returns" in {
          val thriftServerAddr = makeServer(transportFactory) { (a, b) => a + b }

          // ** Set up the client & query the server.
          val service = ClientBuilder()
            .hosts(Seq(thriftServerAddr))
            .codec(codec)
            .hostConnectionLimit(1)
            .build()

          val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

          Await.result(client.add_one(1, 2))
          true must beTrue
        }

        // race condition..
        "handle one-way calls" in {
          val thriftServerAddr = makeServer(transportFactory) { (a, b) => a + b }

          // ** Set up the client & query the server.
          val service = ClientBuilder()
            .hosts(Seq(thriftServerAddr))
            .codec(codec)
            .hostConnectionLimit(1)
            .build()

          val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

          somewayPromise.isDefined must beFalse
          Await.result(client.someway()) must beNull  // returns
          Await.result(somewayPromise) must be_==(())
        }

        "talk to multiple servers" in {
          val NumParties = 10
          val barrier = new CyclicBarrier(NumParties)

          val addrs = 0 until NumParties map { _ =>
            makeServer(transportFactory) { (a, b) => barrier.await(); a + b }
          }

          // ** Set up the client & query the server.
          val service = ClientBuilder()
            .hosts(addrs)
            .codec(codec)
            .hostConnectionLimit(1)
            .build()

          val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

          {
            val futures = 0 until NumParties map { _ => client.multiply(1, 2) }
            val resolved = futures map(Await.result(_))
            resolved foreach { r => r must be_==(3) }
          }
        }
      }

      // Flaky test
      "framed transport" in {
        doit(new TFramedTransport.Factory(), ThriftClientFramedCodec())
      }

      // Flaky test
      "buffered transport" in {
        doit(new TTransportFactory, ThriftClientBufferedCodec())
      }
    }
  }
}
