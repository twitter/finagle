package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.filter.ServerAdmissionControl
import com.twitter.finagle.http.{Fields, Method, Request, Response, Status}
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
object HttpNackFilter {

  /** The `Role` assigned to a `HttpNackFilter` within a `Stack`. */
  val role: Stack.Role = Stack.Role("HttpNack")

  /** Header name for retryable nack responses */
  val RetryableNackHeader: String = "finagle-http-nack"

  /** Header name for non-retryable nack responses */
  val NonRetryableNackHeader: String = "finagle-http-nonretryable-nack"

  /** Header name for requests that have a body and the client can retry */
  val RetryableRequestHeader: String = "finagle-http-retryable-request"

  /** Response status for a nacked request */
  val ResponseStatus: Status = Status.ServiceUnavailable

  private val RetryableNackBody =
    Buf.Utf8("Request was not processed by the server due to an error and is safe to retry")
  private val NonRetryableNackBody =
    Buf.Utf8("Request was not processed by the server and should not be retried")

  private val NonRetryableNackFlags = FailureFlags.Rejected | FailureFlags.NonRetryable
  private val RetryableNackFlags = FailureFlags.Rejected | FailureFlags.Retryable

  private[twitter] object RetryableNack {
    def unapply(t: Throwable): Boolean = t match {
      case f: FailureFlags[_] => f.isFlagged(RetryableNackFlags)
      case _ => false
    }
  }

  private[twitter] object NonRetryableNack {
    def unapply(t: Throwable): Boolean = t match {
      case f: FailureFlags[_] => f.isFlagged(NonRetryableNackFlags)
      case _ => false
    }
  }

  // We consider a `Retry-After: 0` header to also represent a retryable nack.
  // We don't consider values other than 0 since we don't want to make this
  // a problem of this filter.
  private[this] def containsRetryAfter0(rep: Response): Boolean =
    rep.headerMap.get(Fields.RetryAfter) match {
      case Some("0") => true
      case _ => false
    }

  private[finagle] def isRetryableNack(rep: Response): Boolean =
    rep.status == ResponseStatus &&
      (rep.headerMap.contains(RetryableNackHeader) || containsRetryAfter0(rep))

  private[finagle] def isNonRetryableNack(rep: Response): Boolean =
    rep.status == ResponseStatus && rep.headerMap.contains(NonRetryableNackHeader)

  private[finagle] def isNack(rep: Response): Boolean =
    rep.status == ResponseStatus &&
      (rep.headerMap.contains(RetryableNackHeader) || rep.headerMap.contains(
        NonRetryableNackHeader
      ))

  private[finagle] def module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module1[param.Stats, ServiceFactory[Request, Response]] {
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

  /** Construct a new HttpNackFilter */
  private[finagle] def newFilter(statsReceiver: StatsReceiver): SimpleFilter[Request, Response] =
    new HttpNackFilter(statsReceiver)

  private def retryableRequest(req: Request): Boolean = {
    // If the request came in chunked, the finagle HTTP/1.x server implementation will
    // bork the session state if it doesn't consume the body, so we don't nack these if
    // at all possible.
    //
    // If it didn't come in chunked, we don't know if the client has retained a copy of
    // the body and can retry it if we nack it, so we only allow nacking of requests with
    // a body if the client has signaled that it is able to retry them.
    !req.isChunked && (req.content.isEmpty || req.headerMap.contains(RetryableRequestHeader))
  }
}

private final class HttpNackFilter(statsReceiver: StatsReceiver)
    extends SimpleFilter[Request, Response] {
  import HttpNackFilter._

  private[this] val nackCounts = statsReceiver.counter("nacks")
  private[this] val nonRetryableNackCounts = statsReceiver.counter("nonretryable_nacks")

  private[this] val standardHandler = makeHandler(includeBody = true)
  private[this] val bodylessHandler = makeHandler(includeBody = false)

  private[this] def makeHandler(includeBody: Boolean): PartialFunction[Throwable, Response] = {
    // For legacy reasons, this captures all RetryableWriteExceptions
    case RetryPolicy.RetryableWriteException(_) =>
      nackCounts.incr()
      val rep = Response(ResponseStatus)
      rep.headerMap.set(RetryableNackHeader, "true")
      rep.headerMap.set(Fields.RetryAfter, "0")
      if (includeBody) {
        rep.content = RetryableNackBody
      }
      rep

    case NonRetryableNack() =>
      nonRetryableNackCounts.incr()
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

    val isRetryable = retryableRequest(request)
    // We need to strip the header in case this request gets forwarded to another
    // endpoint as the marker header is only valid on a hop-by-hop basis.
    request.headerMap.remove(RetryableRequestHeader)

    if (isRetryable) {
      service(request).handle(handler)
    } else {
      Contexts.local.let(ServerAdmissionControl.NonRetryable, ()) {
        service(request).handle(handler)
      }
    }
  }
}
