package com.twitter.finagle.http.codec

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.{Fields, Request, Response}
import com.twitter.finagle.http.exp.{Multi, StreamTransport}
import com.twitter.finagle.stats.{CategorizingExceptionStatsHandler, StatsReceiver}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Return}

private[finagle] object HttpClientDispatcher {
  private[this] val logger = Logger.get(getClass())
  private[this] val unit: (Unit, Unit) => Unit = (_, _) => ()
  private[this] val exceptionStatsHandler = new CategorizingExceptionStatsHandler()

  def dispatch(
    trans: StreamTransport[Request, Response],
    statsReceiver: StatsReceiver,
    req: Request,
    p: Promise[Response]
  ): Future[Unit] = {
    if (!req.isChunked && !req.headerMap.contains(Fields.ContentLength)) {
      val len = req.content.length
      // Only set the content length if we are sure there is content. This
      // behavior complies with the specification that user agents should not
      // set the content length header for messages without a payload body.
      if (len > 0) req.headerMap.setUnsafe(Fields.ContentLength, len.toString)
    }

    // wait on these concurrently:
    trans
      .write(req).joinWith(
        // Drain the Transport into Response body.
        trans.read().flatMap {
          case Multi(res, readFinished) =>
            p.updateIfEmpty(Return(res))
            readFinished
        } // we don't need to satisfy p when we fail because GenSerialClientDispatcher does already
      )(unit).onFailure { t =>
        // This Future represents the totality of the exchange;
        // thus failure represents *any* failure that can happen
        // during the exchange.
        logger.debug(t, "Failed mid-stream. Terminating stream, closing connection")
        exceptionStatsHandler.record(statsReceiver.scope("stream"), t)
        req.reader.discard()
        trans.close()
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
      statsReceiver
    ) {

  protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] =
    HttpClientDispatcher.dispatch(trans, statsReceiver, req, p)
}
