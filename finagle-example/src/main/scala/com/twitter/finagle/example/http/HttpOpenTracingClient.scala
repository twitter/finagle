package com.twitter.finagle.example.http

import com.lightstep.tracer.jre.JRETracer
import com.lightstep.tracer.shared.Options

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http._
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.tracing.WireTracingFilter
import com.twitter.io.Charsets
import com.twitter.util.Future

import io.opentracing.Tracer
import java.net.InetSocketAddress
import scala.util.Random

/**
 * An example of using OpenTracing Filters with Clients. Below, requests are traced using an
 * OpenTracing tracer and the HTTP OpenTracing client filter. Two parallel requests are
 * made and when they both return (the two Futures are joined) the TCP connection(s)
 * are closed.
 */
object HttpOpenTracingClient {

  def main(args: Array[String]) {

    // ONE-TIME INITIALIZATION
    //
    // Instantiate an OpenTracing implementation and create a Finagle filter
    // to add OpenTracing spans to every request.

    val tracer: Tracer = new JRETracer(new Options("{your_lightstep_token}"))
    val tracingFilter = new OpenTracing.ClientFilter[Request, Response](tracer)

    // Call the example function three times, passing in the tracer
    // and filter above
    for(i <- 1 to 3) {
      makeSomeRequests(tracer, tracingFilter)
    }
  }

  // the request itself, which simply prints the response when it is received
  private[this] def makeRequest(client: Service[Request, Response]) = {
    val request = Request(Version.Http11, Method.Get, "/")
    client(request) onSuccess { response =>
      val responseString = response.contentString
      println("))) Received result for request: " + responseString)
    }
  }

  private[this] def makeSomeRequests(
    tracer: Tracer,
    tracingFilter: OpenTracing.ClientFilter[Request, Response])
  {
    val clientWithoutFilter: Service[Request, Response] = ClientBuilder()
      .codec(Http())
      .hosts(new InetSocketAddress(8080))
      .hostConnectionLimit(1)
      .build()
    val client: Service[Request, Response] = tracingFilter andThen clientWithoutFilter

    // Create a base span to trace the two requests about to be made by this client
    //
    // NOTE: this instrumentation does not rely on any particular
    // OpenTracing implementation
    val span = tracer.buildSpan("root span").start()

    println("))) Issuing requests")

    // make client requests and define the parent span and service name
    // for each request
    val request1 = OpenTracing.letParentSpan(span)(
      OpenTracing.letOperationName("first request")(
        makeRequest(client)
      )
    )
    val request2 = OpenTracing.letParentSpan(span)(
      OpenTracing.letOperationName("second request") (
        makeRequest(client) 
      )
    )

    // When both request1 and request2 have completed, close the TCP connection(s).
    (request1 join request2) ensure {
      client.close()
      span.finish()
    }
  }
}