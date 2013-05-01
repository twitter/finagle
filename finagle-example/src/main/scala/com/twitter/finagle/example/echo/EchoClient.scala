package com.twitter.finagle.example.echo

import com.twitter.finagle.builder.ClientBuilder
import java.net.InetSocketAddress
import com.twitter.finagle.Service

class EchoClient(id: Int) {
    // Construct a client, and connect it to localhost:8080
  val client: Service[String, String] = ClientBuilder()
    .codec(StringCodec)
    .hosts(new InetSocketAddress(8090))
    .hostConnectionLimit(1)
    .keepAlive(true)
    .build()

  def apply(msg: String) {
    // Issue a newline-delimited request, respond to the result
    // asynchronously:
    client(msg) onSuccess { result =>
      println("Received result asynchronously: " + result + " on " + id)
    } onFailure { error =>
      error.printStackTrace()
    } ensure {
      // All done! Close TCP connection(s):
      //client.close()
    }
  }
}
