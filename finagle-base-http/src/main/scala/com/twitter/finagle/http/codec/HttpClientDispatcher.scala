package com.twitter.finagle.http.codec

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.Multi
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.StreamTransport
import com.twitter.finagle.stats.CategorizingExceptionStatsHandler
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return

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
