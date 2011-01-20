package com.twitter.finagle.thrift

import java.net.ServerSocket
import java.util.logging
import java.util.concurrent.CyclicBarrier

import org.specs.Specification

import org.apache.thrift.transport.{TServerSocket, TFramedTransport}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.async.AsyncMethodCallback

import com.twitter.test.{B, AnException, SomeStruct}
import com.twitter.util.{RandomSocket, Promise, Return, Throw, Future}

import com.twitter.finagle.builder.ClientBuilder

object FinagleClientThriftServerSpec extends Specification {
  "finagle client vs. synchronous thrift server" should {
    def makeServer(f: (Int, Int) => Int) = {
      val processor = new B.Iface {
        def multiply(a: Int, b: Int): Int = f(a, b)
        def add(a: Int, b: Int): Int = f(a, b)
        def add_one(a: Int, b: Int) { throw new AnException }
        def complex_return(someString: String) = new SomeStruct(123, someString)
      }

      val (thriftServerAddr, thriftServer) = {
        val serverAddr = RandomSocket()
        val socket = new ServerSocket(serverAddr.getPort)
        socket.setReuseAddress(true)
        val serverSocketTransport = new TServerSocket(socket)

        val server = new TSimpleServer(
          new B.Processor(processor),
          serverSocketTransport,
          new TFramedTransport.Factory(),
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

    "talk to each other" in {
      // TODO: interleave requests (to test seqids, etc.)

      val thriftServerAddr = makeServer { (a, b) => a + b }

      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(Seq(thriftServerAddr))
        .codec(ThriftFramedTransportCodec())
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      val future = client.add(1, 2)
      future() must be_==(3)
    }

    "handle exceptions" in {
      val thriftServerAddr = makeServer { (a, b) => a + b }
      
      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(Seq(thriftServerAddr))
        .codec(ThriftFramedTransportCodec())
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      client.add_one(1, 2)() must throwA[AnException]
    }

    "talk to multiple servers" in {
      val NumParties = 10
      val barrier = new CyclicBarrier(NumParties)

      val addrs = 0 until NumParties map { _ =>
        makeServer { (a, b) => barrier.await(); a + b }
      }

      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(addrs)
        .codec(ThriftFramedTransportCodec())
        .build()

      val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

      {
        val futures = 0 until NumParties map { _ => client.add(1, 2) }
        val resolved = futures map(_())
        resolved foreach { r => r must be_==(3) }
      }
    }
  }
}
