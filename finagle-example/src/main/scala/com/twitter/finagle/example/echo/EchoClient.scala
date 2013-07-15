package com.twitter.finagle.example.echo

import com.twitter.finagle.builder.ClientBuilder
import java.net.InetSocketAddress
import com.twitter.finagle.Service

object EchoClient {
  def main(args: Array[String]) {
    // Construct a client, and connect it to localhost:8080
    val client: Service[String, String] = ClientBuilder()
      .codec(StringCodec)
      .hosts(new InetSocketAddress(8080))
      .hostConnectionLimit(1)
      .build()

    // Issue a newline-delimited request, respond to the result
    // asynchronously:
    client("hi mom\n") onSuccess { result =>
      println("Received result asynchronously: " + result)
    } onFailure { error =>
      error.printStackTrace()
    } ensure {
      // All done! Close TCP connection(s):
      client.close()
    }
  }
}
