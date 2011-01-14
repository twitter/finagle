package com.twitter.finagle.thrift

import java.net.ServerSocket
import java.util.logging

import org.specs.Specification

import org.apache.thrift.transport.{TServerSocket, TFramedTransport}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.async.AsyncMethodCallback

import com.twitter.test.Arithmetic
import com.twitter.util.{RandomSocket, Promise, Return, Throw}

import com.twitter.finagle.builder.ClientBuilder

object FinagleClientThriftServerSpec extends Specification {
  object processor extends Arithmetic.Iface {
    def add(a: Int, b: Int): Int = a + b
  }

  "finagle client vs. synchronous thrift server" should {
    "talk to each other" in {
      val (thriftServerAddr, thriftServer) = {
        val serverAddr = RandomSocket()
        val socket = new ServerSocket(serverAddr.getPort)
        socket.setReuseAddress(true)
        val serverSocketTransport = new TServerSocket(socket)

        val server = new TSimpleServer(
          new Arithmetic.Processor(processor),
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

      // ** Set up the client & query the server.
      val service = ClientBuilder()
        .hosts(Seq(thriftServerAddr))
        .codec(new Thrift())
        .build()

      val client = new Arithmetic.ServiceToClient(service, new TBinaryProtocol.Factory())

      val future = client.add(1, 2)
      future() must be_==(3)
    }
  }
}
