package com.twitter.finagle.http.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.{Fields, Request, Response}
import com.twitter.finagle.http.exp.{Multi, StreamTransport}
import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Reader
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Return, Throw}

private[http] object HttpClientDispatcher {

  private val log = Logger(getClass.getName)

  private def swallowNackBody(response: Response): Future[Unit] = {
    // It's not guaranteed that the body of a nack response will be fully buffered even though
    // it is small and will have a content-length header: if the client is set to stream and
    // the message happens to hit the buffer in a way that separates the response prelude and
    // the body netty will send it as chunked anyway. Therefore, we need to manually discard
    // it in such situations.
    if (response.isChunked) {
      def swallowBody(reader: Reader, maxRead: Int): Future[Unit] = {
        // We add 1 to `readMax` in order to detect large responses earlier
        // and avoid falling into an infinite loop of `read(0)` calls.
        reader.read(maxRead + 1).flatMap {
          case Some(msg) if msg.length <= maxRead =>
            swallowBody(reader, maxRead - msg.length)

          case Some(_) =>
            val msg = "Received an excessively large nack response body."
            val ex = new Exception(msg)
            log.warning(ex, msg)
            Future.exception(ex)

          case None => Future.Done
        }
      }

      // We don't want to try swallowing an infinite sized message body so we bound it
      // to 1 KB, which should be more than enough as the message body is intended to be
      // a one liner saying that the response is a nack response. If the message body is
      // larger than 1 KB, we abort and log a warning.
      swallowBody(response.reader, 1024)
        .onFailure(_ => response.reader.discard())
    } else {
      Future.Done
    }
  }
}

/**
 * Client dispatcher for HTTP.
 *
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
private[finagle] class HttpClientDispatcher(
    trans: StreamTransport[Request, Response],
    statsReceiver: StatsReceiver)
  extends GenSerialClientDispatcher[Request, Response, Request, Multi[Response]](
    trans,
    statsReceiver) {

  import HttpClientDispatcher._

  protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] = {
    if (!req.isChunked && !req.headerMap.contains(Fields.ContentLength)) {
      val len = req.content.length
      // Only set the content length if we are sure there is content. This
      // behavior complies with the specification that user agents should not
      // set the content length header for messages without a payload body.
      if (len > 0) req.headerMap.set(Fields.ContentLength, len.toString)
    }

    // wait on these concurrently:
    Future.join(Seq(
      trans.write(req),
      // Drain the Transport into Response body.
      trans.read().flatMap {
        case Multi(res, readFinished) if HttpNackFilter.isRetryableNack(res) =>
          p.updateIfEmpty(Throw(Failure.RetryableNackFailure))
          swallowNackBody(res).before(readFinished)

        case Multi(res, readFinished) if HttpNackFilter.isNonRetryableNack(res) =>
          p.updateIfEmpty(Throw(Failure.NonRetryableNackFailure))
          swallowNackBody(res).before(readFinished)

        case Multi(res, readFinished) =>
          p.updateIfEmpty(Return(res))
          readFinished
      } // we don't need to satisfy p when we fail because GenSerialClientDispatcher does already
    )).onFailure { _ =>
      // This Future represents the totality of the exchange;
      // thus failure represents *any* failure that can happen
      // during the exchange.
      req.reader.discard()
      trans.close()
    }
  }
}
