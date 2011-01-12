package com.twitter.finagle.test

import java.util.logging.Logger
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.builder.{ClientBuilder, Http}
import com.twitter.finagle.service.Service

object HttpClient {
  def main(args: Array[String]) {
    val client =
      ClientBuilder()
        .name("http")
        .hosts("localhost:10000,localhost:10001,localhost:10003")
        .codec(Http)
        .retries(2)
        .logger(Logger.getLogger("http"))
        .build()

    for (_ <- 0 until 100)
      makeRequest(client)
  }

  def makeRequest(client: Service[HttpRequest, HttpResponse]) {
    client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) respond {
      case _ =>
        makeRequest(client)
    }
  }

  def quiesce() = ()
  def shutdown() = ()
}
