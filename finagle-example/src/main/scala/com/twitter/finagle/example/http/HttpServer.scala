package com.twitter.finagle.example.http

import com.twitter.finagle.http.{Request, Response, Status, Version, Fields}
import com.twitter.finagle.{Http, ListeningServer, Service, SimpleFilter}
import com.twitter.util.Future
import java.net.InetSocketAddress

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
  class HandleExceptions extends SimpleFilter[Request, Response] {
    def apply(request: Request, service: Service[Request, Response]) = {

      // `handle` asynchronously handles exceptions.
      service(request) handle {
        case error =>
          val statusCode = error match {
            case _: IllegalArgumentException =>
              Status.Forbidden
            case _ =>
              Status.InternalServerError
          }
          val errorResponse = Response(Version.Http11, statusCode)
          errorResponse.contentString = error.getStackTrace.mkString("\n")

          errorResponse
      }
    }
  }

  /**
   * A simple Filter that checks that the request is valid by inspecting the
   * "Authorization" header.
   */
  class Authorize extends SimpleFilter[Request, Response] {
    def apply(request: Request, continue: Service[Request, Response]) = {
      if (Some("open sesame") == request.headerMap.get(Fields.Authorization)) {
        continue(request)
      } else {
        Future.exception(new IllegalArgumentException("You don't know the secret"))
      }
    }
  }

  /**
   * The service itself. Simply echos back "hello world"
   */
  class Respond extends Service[Request, Response] {
    def apply(request: Request) = {
      val response = Response(Version.Http11, Status.Ok)
      response.contentString = "hello world"
      Future.value(response)
    }
  }

  def main(args: Array[String]): Unit = {
    val handleExceptions = new HandleExceptions
    val authorize = new Authorize
    val respond = new Respond

    // compose the Filters and Service together:
    val myService: Service[Request, Response] = handleExceptions andThen authorize andThen respond

    val server: ListeningServer =
      Http.server.withLabel("httpserver").serve(new InetSocketAddress(8080), myService)
  }
}
