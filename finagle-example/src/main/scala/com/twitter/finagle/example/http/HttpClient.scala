package com.twitter.finagle.example.http

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.io.Charsets
import com.twitter.util.Future

/**
 * A somewhat advanced example of using Filters with Clients. Below, HTTP 4xx and 5xx
 * class requests are converted to Exceptions. Additionally, two parallel requests are
 * made and when they both return (the two Futures are joined) the TCP connection(s)
 * are closed.
 */
object HttpClient {
  class InvalidAsk extends Exception

  /**
   * Convert HTTP 4xx and 5xx class responses into Exceptions.
   */
  class HandleErrors extends SimpleFilter[HttpAsk, HttpResponse] {
    def apply(request: HttpAsk, service: Service[HttpAsk, HttpResponse]) = {
      // flatMap asynchronously responds to requests and can "map" them to both
      // success and failure values:
      service(request) flatMap { response =>
        response.getStatus match {
          case OK        => Future.value(response)
          case FORBIDDEN => Future.exception(new InvalidAsk)
          case _         => Future.exception(new Exception(response.getStatus.getReasonPhrase))
        }
      }
    }
  }

  def main(args: Array[String]) {
    val clientWithoutErrorHandling: Service[HttpAsk, HttpResponse] = ClientBuilder()
      .codec(Http())
      .hosts(new InetSocketAddress(8080))
      .hostConnectionLimit(1)
      .build()

    val handleErrors = new HandleErrors

    // compose the Filter with the client:
    val client: Service[HttpAsk, HttpResponse] = handleErrors andThen clientWithoutErrorHandling

    println("))) Issuing two requests in parallel: ")
    val request1 = makeAuthorizedAsk(client)
    val request2 = makeUnauthorizedAsk(client)

    // When both request1 and request2 have completed, close the TCP connection(s).
    (request1 join request2) ensure {
      client.close()
    }
  }

  private[this] def makeAuthorizedAsk(client: Service[HttpAsk, HttpResponse]) = {
    val authorizedAsk = new DefaultHttpAsk(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    authorizedAsk.headers().add(HttpHeaders.Names.AUTHORIZATION, "open sesame")

    client(authorizedAsk) onSuccess { response =>
      val responseString = response.getContent.toString(Charsets.Utf8)
      println("))) Received result for authorized request: " + responseString)
    }
  }

  private[this] def makeUnauthorizedAsk(client: Service[HttpAsk, HttpResponse]) = {
    val unauthorizedAsk = new DefaultHttpAsk(
      HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

    // use the onFailure callback since we convert HTTP 4xx and 5xx class
    // responses to Exceptions.
    client(unauthorizedAsk) onFailure { error =>
      println("))) Unauthorized request errored (as desired): " + error.getClass.getName)
    }
  }
}
