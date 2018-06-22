package com.twitter.finagle.example.http

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.{Http, Service, SimpleFilter}
import com.twitter.finagle.http.{Request, Response, Status, Version, Method, Fields}
import com.twitter.util.Future
import java.net.InetSocketAddress

/**
 * A somewhat advanced example of using Filters with Clients. Below, HTTP 4xx and 5xx
 * class requests are converted to Exceptions. Additionally, two parallel requests are
 * made and when they both return (the two Futures are joined) the TCP connection(s)
 * are closed.
 */
object HttpClient {
  class InvalidRequest extends Exception

  /**
   * Convert HTTP 4xx and 5xx class responses into Exceptions.
   */
  class HandleErrors extends SimpleFilter[Request, Response] {
    def apply(request: Request, service: Service[Request, Response]) = {
      // flatMap asynchronously responds to requests and can "map" them to both
      // success and failure values:
      service(request) flatMap { response =>
        response.status match {
          case Status.Ok => Future.value(response)
          case Status.Forbidden => Future.exception(new InvalidRequest)
          case _ => Future.exception(new Exception(response.status.reason))
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val clientWithoutErrorHandling: Service[Request, Response] = ClientBuilder()
      .stack(Http.client)
      .hosts(new InetSocketAddress(8080))
      .hostConnectionLimit(1)
      .build()

    val handleErrors = new HandleErrors

    // compose the Filter with the client:
    val client: Service[Request, Response] = handleErrors andThen clientWithoutErrorHandling

    println("))) Issuing two requests in parallel: ")
    val request1 = makeAuthorizedRequest(client)
    val request2 = makeUnauthorizedRequest(client)

    // When both request1 and request2 have completed, close the TCP connection(s).
    (request1 join request2) ensure {
      client.close()
    }
  }

  private[this] def makeAuthorizedRequest(client: Service[Request, Response]) = {
    val authorizedRequest = Request(Version.Http11, Method.Get, "/")
    authorizedRequest.headerMap.add(Fields.Authorization, "open sesame")

    client(authorizedRequest) onSuccess { response =>
      val responseString = response.contentString
      println("))) Received result for authorized request: " + responseString)
    }
  }

  private[this] def makeUnauthorizedRequest(client: Service[Request, Response]) = {
    val unauthorizedRequest = Request(Version.Http11, Method.Get, "/")

    // use the onFailure callback since we convert HTTP 4xx and 5xx class
    // responses to Exceptions.
    client(unauthorizedRequest) onFailure { error =>
      println("))) Unauthorized request errored (as desired): " + error.getClass.getName)
    }
  }
}
