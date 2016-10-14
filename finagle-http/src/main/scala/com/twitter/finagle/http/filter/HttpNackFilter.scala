package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.service.RetryPolicy
import com.twitter.io.Buf
import com.twitter.util.Future

/**
 * When a server fails with retryable failures, it sends back a
 * `NackResponse`, i.e. a 503 response code with "finagle-http-nack"
 * header. A non-retryable failure will be converted to a 503 with
 * "finagle-http-nonretryable-nack".
 *
 * Clients who recognize the header can handle the response appropriately.
 * Clients who don't recognize the header treat the response the same way as
 * other 503 response.
 */
private[finagle] object HttpNackFilter {
  val role: Stack.Role = Stack.Role("HttpNack")

  val RetryableNackHeader: String = "finagle-http-nack"
  val NonRetryableNackHeader: String = "finagle-http-nonretryable-nack"
  val ResponseStatus: Status = Status.ServiceUnavailable

  private val RetryableNackBody = Buf.Utf8("Request was not processed by the server due to an error and is safe to retry")
  private val NonRetryableNackBody = Buf.Utf8("Request was not processed by the server and should not be retried")

  private val NonRetryableNackFlags = Failure.Rejected|Failure.NonRetryable

  def isRetryableNack(rep: Response): Boolean =
    rep.status == ResponseStatus && rep.headerMap.contains(RetryableNackHeader)

  def isNonRetryableNack(rep: Response): Boolean =
    rep.status == ResponseStatus && rep.headerMap.contains(NonRetryableNackHeader)

  def module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module1[param.Stats,ServiceFactory[Request, Response]] {
      val role: Stack.Role = HttpNackFilter.role
      val description = "Convert rejected requests to 503s, respecting retryability"

      def make(
        _stats: param.Stats,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] = {
        val param.Stats(stats) = _stats
        new HttpNackFilter(stats).andThen(next)
      }
    }
}

private[finagle] class HttpNackFilter(statsReceiver: StatsReceiver)
  extends SimpleFilter[Request, Response] {
  import HttpNackFilter._

  private[this] val nackCounts = statsReceiver.counter("nacks")
  private[this] val nonretryableNackCounts = statsReceiver.counter("nonretryable_nacks")

  private[this] val standardHandler = makeHandler(true)
  private[this] val bodylessHandler = makeHandler(false)

  private[this] def makeHandler(includeBody: Boolean): PartialFunction[Throwable, Response] = {
    case RetryPolicy.RetryableWriteException(_) =>
      nackCounts.incr()
      val rep = Response(ResponseStatus)
      rep.headerMap.set(RetryableNackHeader, "true")
      if (includeBody) {
        rep.content = RetryableNackBody
      }
      rep

    case f: Failure if f.isFlagged(NonRetryableNackFlags) =>
      nonretryableNackCounts.incr()
      val rep = Response(ResponseStatus)
      rep.headerMap.set(NonRetryableNackHeader, "true")
      if (includeBody) {
        rep.content = NonRetryableNackBody
      }
      rep
  }

  def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    val handler =
      if (request.method == Method.Head) bodylessHandler
      else standardHandler

    service(request).handle(handler)
  }
}
