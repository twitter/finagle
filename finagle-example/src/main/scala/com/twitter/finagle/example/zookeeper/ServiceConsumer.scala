package com.twitter.finagle.example.zookeeper

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.twitter.finagle.Http
import com.twitter.finagle.example.zookeeper.ServiceProvider.EchoServer
import com.twitter.finagle.http.{Method, Request, Version}
import com.twitter.io.Reader
import com.twitter.util.Await

/**
 * This demo shows how to consume the service in zookeeper.
 *
 * Run ServiceProvider before ServiceConsumer
 *
 */
object ServiceConsumer {

  def main(args: Array[String]): Unit = {

    //use zookeeper to discover service
    val client = Http.client.withSessionPool
      .maxSize(10)
      .newService(ServiceProvider.buildConsumerPath(EchoServer.servicePath), "echo-client")

    //create a "Greetings!" request.
    val data =
      Reader.fromStream(new ByteArrayInputStream("Greetings!".getBytes(StandardCharsets.UTF_8)))
    val request = Request(Version.Http11, Method.Post, "/", data)

    Await.ready(client(request)) onSuccess { response =>
      println(s"response status: ${response.status}, response string: ${response.contentString} ")
    } onFailure { e =>
      println(s"error: $e")
    } ensure {
      client.close()
    }

  }
}
