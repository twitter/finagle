package com.twitter.finagle.example.thrift

import java.net.InetSocketAddress

import org.apache.thrift.protocol.TBinaryProtocol

import com.twitter.finagle.builder.{ServerBuilder, Server}
import com.twitter.finagle.example.thriftscala.Hello
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.util.{Await, Future}

object ThriftServer {
  def main(args: Array[String]) {
    //#thriftserverapi
    val impl = new Hello.FutureIface {
      def hi() = {
        Future("hi")
      }
    }

    val service = new Hello.FinagledService(impl, new TBinaryProtocol.Factory())

    val server: Server = ServerBuilder()
      .bindTo(new InetSocketAddress(8080))
      .codec(ThriftServerFramedCodec())
      .name(Hello.toString)
      .build(service)
    //#thriftserverapi
  }
}
