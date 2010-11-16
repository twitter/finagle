package com.twitter.finagle.test

import java.util.concurrent.TimeUnit

import org.jboss.netty.handler.codec.http._

import net.lag.configgy.{Configgy, RuntimeEnvironment}
import com.twitter.ostrich
import com.twitter.finagle.builder.{ClientBuilder, Http, Ostrich}
import com.twitter.finagle.stub.Stub

import com.twitter.util.{Return, Throw}

object HttpClient extends ostrich.Service {
  def main(args: Array[String]) {
    val runtime = new RuntimeEnvironment(getClass)
    runtime.load(args)
    val config = Configgy.config
    ostrich.ServiceTracker.register(this)
    ostrich.ServiceTracker.startAdmin(config, runtime)

    val client =
      ClientBuilder()
        .name("http")
        .hosts("localhost:10000,localhost:10001,localhost:10003")
        .codec(Http)
        .exportLoadsToOstrich()
        .reportTo(Ostrich())
        .retries(100)
        .buildStub[HttpRequest, HttpResponse]()

    for (_ <- 0 until 100)
      makeRequest(client)
  }

  def makeRequest(client: Stub[HttpRequest, HttpResponse]) {
    client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) respond {
      case _ =>
        makeRequest(client)
    }
  }

  def quiesce() = ()
  def shutdown() = ()
}
