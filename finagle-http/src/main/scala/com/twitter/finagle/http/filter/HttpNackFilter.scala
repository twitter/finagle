package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.http.{Response, Status}
import com.twitter.finagle.service.RetryPolicy
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponseStatus, HttpResponse}

/**
 * When a server fails with retryable failures, it sends back a
 * `NackResponse`, i.e. a 503 response code with "finagle-http-nack"
 * header.
 *
 * Clients who recognize the header convert the response to a
 * restartable failure, which can be retried. Clients who don't
 * recognize the header treats the response the same way as other
 * 503 response.
 */
private[finagle] object HttpNackFilter {
  val Header: String = "finagle-http-nack"
  val ResponseStatus: HttpResponseStatus = Status.ServiceUnavailable

  private val NackResponse: Response = {
    val rep = Response(ResponseStatus)
    rep.headers.set(Header, "true")
    rep.write("Request was not processed by the server due to an error and is safe to retry")
    rep
  }

  def isNack(rep: HttpResponse): Boolean =
    rep.getStatus.getCode == ResponseStatus.getCode && rep.headers.contains(Header)
}

private[finagle] class HttpNackFilter extends SimpleFilter[HttpRequest, HttpResponse] {
  import HttpNackFilter._
  def apply(
    request: HttpRequest, service: Service[HttpRequest, HttpResponse]
  ): Future[HttpResponse] =
    service(request).handle {
      case RetryPolicy.RetryableWriteException(_) =>
        NackResponse
    }
}