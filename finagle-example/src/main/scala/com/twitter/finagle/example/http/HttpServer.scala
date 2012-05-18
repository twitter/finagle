package com.twitter.finagle.example.http

import com.twitter.finagle.{Service, SimpleFilter}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.util.CharsetUtil.UTF_8
import com.twitter.util.Future
import java.net.InetSocketAddress
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http.Http

/**
 * This example demonstrates a sophisticated HTTP server that handles exceptions
 * and performs authorization via a shared secret. The exception handling and
 * authorization code are written as Filters, thus isolating these aspects from
 * the main service (here called "Respond") for better code organization.
 */
object HttpServer {
  /**
   * A simple Filter that catches exceptions and converts them to appropriate
   * HTTP responses.
   */
  class HandleExceptions extends SimpleFilter[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {

      // `handle` asynchronously handles exceptions.
      service(request) handle { case error =>
        val statusCode = error match {
          case _: IllegalArgumentException =>
            FORBIDDEN
          case _ =>
            INTERNAL_SERVER_ERROR
        }
        val errorResponse = new DefaultHttpResponse(HTTP_1_1, statusCode)
        errorResponse.setContent(copiedBuffer(error.getStackTraceString, UTF_8))

        errorResponse
      }
    }
  }

  /**
   * A simple Filter that checks that the request is valid by inspecting the
   * "Authorization" header.
   */
  class Authorize extends SimpleFilter[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest, continue: Service[HttpRequest, HttpResponse]) = {
      if ("open sesame" == request.getHeader("Authorization")) {
        continue(request)
      } else {
        Future.exception(new IllegalArgumentException("You don't know the secret"))
      }
    }
  }

  /**
   * The service itself. Simply echos back "hello world"
   */
  class Respond extends Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = {
      val response = new DefaultHttpResponse(HTTP_1_1, OK)
      response.setContent(copiedBuffer("hello world", UTF_8))
      Future.value(response)
    }
  }

  def main(args: Array[String]) {
    val handleExceptions = new HandleExceptions
    val authorize = new Authorize
    val respond = new Respond

    // compose the Filters and Service together:
    val myService: Service[HttpRequest, HttpResponse] = handleExceptions andThen authorize andThen respond

    val server: Server = ServerBuilder()
      .codec(Http())
      .bindTo(new InetSocketAddress(8080))
      .name("httpserver")
      .build(myService)
  }
}
