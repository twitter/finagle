package com.twitter.finagle.example.echo

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.util.Future
import java.net.InetSocketAddress
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.channel.OpenConnectionsThresholds
import com.twitter.util.{Future, CountDownLatch}

object EchoServer {
  //var latch = new CountDownLatch(1)
  def start() = {
    /**
     * A very simple service that simply echos its request back
     * as a response. Note that it returns a Future, since everything
     * in Finagle is asynchronous.
     */
    val service = new Service[String, String] {
      def apply(request: String) = {
    //    latch.await()
        Future.value(request)
      }
    }

    val threshold = OpenConnectionsThresholds(
      lowWaterMark = 1,
      highWaterMark = 2,
      idleTimeout = 5 seconds
    )

    // Bind the service to port 8080
    ServerBuilder()
      .codec(StringCodec)
      .bindTo(new InetSocketAddress(8090))
      .name("echoserver")
      .openConnectionsThresholds(threshold)
      .build(service)
  }
}
