package com.twitter.finagle.example.echo

import com.twitter.finagle.builder.ClientBuilder
import java.net.InetSocketAddress

object EchoClient {
  def main(args: Array[String]) {
    val client = ClientBuilder()
      .codec(StringCodec)
      .hosts(new InetSocketAddress(8080))
      .build()

    // Issue request:
    client("hi mom\n") onSuccess { result =>
      println("Received result: " + result)
    } onFailure { error =>
      error.printStackTrace()
    } ensure {
      // All done! Close TCP connection:
      println("releasin")
      client.release()
    }
  }
}