package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.io.{Buf, Reader}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Return, Throw, Try}

/**
 * Filter for converting the HTTP nack representation to a [[Failure]]
 *
 * @note this filter attempts to swallow up to 1 KB of the nack response body
 *       so that HTTP/1.x connections can be reused.
 */
private[http] final class ClientNackFilter extends SimpleFilter[Request, Response] {
  import ClientNackFilter._

  def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    // If the request was chunked, we likely consume some of the body during an initial
    // dispatch so it's not generally safe to retry even if the server says it is.
    if (request.isChunked) {
      // Make sure we don't accidentally signal that we can retry this request. This
      // could happen if, for example, the same request gets reused but with a different
      // body representation.
      request.headerMap.remove(HttpNackFilter.RetryableRequestHeader)
      service(request).transform(convertChunkedReqNackFn)
    } else {
      if (!request.content.isEmpty) {
        // We add the `CanRetryWithBodyHeader` to signal to the server that we're able
        // to retry this request with a body.
        request.headerMap.setUnsafe(HttpNackFilter.RetryableRequestHeader, "")
      }
      service(request).transform(convertNackFn)
    }
  }
}

object ClientNackFilter {
  private[this] val log = Logger(getClass.getName)

  private[this] val respondRetryableFailure: Try[Unit] => Future[Response] = { _ =>
    Future.exception(Failure.RetryableNackFailure)
  }

  private[this] val respondNonRetryableFailure: Try[Unit] => Future[Response] = { _ =>
    Future.exception(Failure.NonRetryableNackFailure)
  }

  // note: in the nack case we use `transform` since we've already
  // decided it's a nack response regardless of if we succeed in
  // swallowing the body.
  private val convertNackFn: Try[Response] => Future[Response] = {
    case Return(res) if HttpNackFilter.isRetryableNack(res) =>
      swallowNackResponse(res).transform(respondRetryableFailure)

    case Return(res) if HttpNackFilter.isNonRetryableNack(res) =>
      swallowNackResponse(res).transform(respondNonRetryableFailure)

    case t => Future.const(t)
  }

  // It's likely unsafe to retry this request based on request body being chunked so
  // we don't mark it retryable even if the server thinks it's safe.
  private val convertChunkedReqNackFn: Try[Response] => Future[Response] = {
    case Return(res) if HttpNackFilter.isNack(res) =>
      swallowNackResponse(res).transform(respondNonRetryableFailure)

    case Throw(ex: FailureFlags[_]) =>
      Future.exception(ex.asNonRetryable)

    // Everything else passes through
    case t => Future.const(t)
  }

  /**
   * A stack module that converts HTTP nack responses to Failures
   */
  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      private[this] val nackFilter = new ClientNackFilter
      val role: Stack.Role = Stack.Role("ClientNackFilter")
      val description: String = "Convert HTTP nack responses to Failures"
      def make(next: ServiceFactory[Request, Response]): ServiceFactory[Request, Response] =
        nackFilter.andThen(next)
    }

  private[this] def swallowNackResponse(response: Response): Future[Unit] = {
    // It's not guaranteed that the body of a nack response will be fully buffered even though
    // it is small and will have a content-length header: if the client is set to stream and
    // the message happens to hit the buffer in a way that separates the response prelude and
    // the body netty will send it as chunked anyway. Therefore, we need to manually discard
    // it in such situations.
    if (!response.isChunked) Future.Done
    else {
      // We don't want to try swallowing an infinite sized message body so we bound it
      // to 1 KB, which should be more than enough as the message body is intended to be
      // a one liner saying that the response is a nack response. If the message body is
      // larger than 1 KB, we abort and log a warning.
      swallowNackBody(response.reader, 1024)
        .onFailure(_ => response.reader.discard())
    }
  }

  private[this] def swallowNackBody(reader: Reader[Buf], maxRead: Int): Future[Unit] =
    reader.read().flatMap {
      case Some(chunk) if chunk.length <= maxRead =>
        swallowNackBody(reader, maxRead - chunk.length)

      case Some(_) =>
        log.warning("Received an excessively large nack response body.")
        reader.discard() // Since we didn't swallow the whole body we discard the reader
        Future.Done

      case None => Future.Done
    }
}
