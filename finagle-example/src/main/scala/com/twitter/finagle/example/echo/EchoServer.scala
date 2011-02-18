package com.twitter.finagle.example.echo

import com.twitter.finagle.Service
import com.twitter.util.Future
import com.twitter.finagle.builder.ServerBuilder
import java.net.InetSocketAddress

object EchoServer {
  def main(args: Array[String]) {
    val service = new Service[String, String] {
      def apply(request: String) = Future.value(request)
    }

    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(new InetSocketAddress(8080))
      .build(service)
  }
}