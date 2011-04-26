package com.twitter.finagle.thrift

import java.net.ServerSocket
import java.util.logging
import java.util.concurrent.CyclicBarrier

import org.specs.Specification

import org.apache.thrift.transport.{TServerSocket, TFramedTransport, TTransportFactory}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.async.AsyncMethodCallback

import com.twitter.test.{B, AnException, SomeStruct}
import com.twitter.util.{RandomSocket, Promise, Return, Throw, Future}

import com.twitter.finagle.{Codec, ClientCodec}
import com.twitter.finagle.builder.ClientBuilder

object FinagleClientThriftServerSpec extends Specification {
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
          Future.void
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
      codec: ClientCodec[ThriftClientRequest, Array[Byte]]
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
        future() must be_==(3)
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
       
        client.add(1, 2)() must throwA[AnException]
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
       
        client.add_one(1, 2)()
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
        client.someway()() must beNull  // returns
        somewayPromise() must be_==(())
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
          val resolved = futures map(_())
          resolved foreach { r => r must be_==(3) }
        }
      }
    }

    "framed transport" in {
      doit(new TFramedTransport.Factory(), ThriftClientFramedCodec())
    }

    "buffered transport" in {
      doit(new TTransportFactory, new ThriftClientBufferedCodec(new TBinaryProtocol.Factory))
    }
  }
}
