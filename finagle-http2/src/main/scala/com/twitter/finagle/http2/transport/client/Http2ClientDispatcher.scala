package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.dispatch.ClientDispatcher
import com.twitter.finagle.http.codec.HttpClientDispatcher
import com.twitter.finagle.http.exp.{Multi, StreamTransport}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Future, Promise}

/**
 * Client dispatcher for HTTP/2.
 *
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
private class Http2ClientDispatcher(
  trans: StreamTransport[Request, Response],
  statsReceiver: StatsReceiver)
    extends ClientDispatcher[Request, Response, Request, Multi[Response]](trans) {

  protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] =
    HttpClientDispatcher.dispatch(trans, statsReceiver, req, p)
}
