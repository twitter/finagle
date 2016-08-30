package com.twitter.finagle.example.http

import com.lightstep.tracer.jre.JRETracer
import com.lightstep.tracer.shared.Options

import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http._
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.io.Charsets.Utf8
import com.twitter.util.Future
import java.net.InetSocketAddress

/**
 * This example demonstrates tracing a server response using an
 * OpenTracing tracer and an HTTP OpenTracing server filter.
 */
object HttpOpenTracingServer {

  /**
   * The service itself. Simply waits 1 second
   * and echos back "hello world"
   */
  class Respond extends Service[Request, Response] {
    def apply(request: Request) = {
      val response = Response(Version.Http11, Status.Ok)
      response.contentString = "hello world"
      Thread.sleep(1000)
      Future.value(response)
    }
  }

  def main(args: Array[String]) {

    val respond = new Respond

    // Instantiate an OpenTracing implementation and create a Finagle filter
    // to add OpenTracing spans to every response.

    val tracer = new JRETracer(new Options("{your_lightstep_token}"))
    val traceResponse = new OpenTracing.ServerFilter[Request, Response](tracer)

    // compose the Filters and Service together:
    val myService: Service[Request, Response] = traceResponse andThen respond

    val server: Server = ServerBuilder()
      .codec(Http())
      .bindTo(new InetSocketAddress(8080))
      .name("httpserver")
      .build(myService)
  }
}
