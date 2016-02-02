package com.twitter.finagle.example.zookeeper

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.example.zookeeper.ServiceProvider.EchoServer
import com.twitter.finagle.http._
import com.twitter.io.Reader
import com.twitter.util.Future

/**
  * This demo shows how to consume the service in zookeeper.
  *
  * Run ServiceProvider before ServiceConsumer
  *
  */
object ServiceConsumer {

  def main(args: Array[String]): Unit = {

    //use zookeeper to discover service
    val client: Service[Request, Response] = ClientBuilder()
      .codec(Http())
      .hostConnectionLimit(10)
      .dest(ServiceProvider.buildConsumerPath(EchoServer.servicePath))
      .build()

    //create a "Greetings!" request.
    val data = Reader.fromStream(new ByteArrayInputStream("Greetings!".getBytes(StandardCharsets.UTF_8)))
    val request = Request(Version.Http11, Method.Post, "/", data)

    client(request) onSuccess { response =>
      println(s"response status: ${response.status}, response string: ${response.contentString} ")
    } onFailure { e =>
      println(s"error: $e")
    } ensure {
      client.close()
    }
  }
}
