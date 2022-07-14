package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.dispatch.ClientDispatcher
import com.twitter.finagle.http.ClientEndpointer.TransportModifier
import com.twitter.finagle.http.codec.HttpClientDispatcher
import com.twitter.finagle.http2.transport.client.Http2Transport
import com.twitter.finagle.http2.transport.client.StreamChannelTransport
import com.twitter.finagle.netty4.http.Netty4ClientStreamTransport
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Future
import com.twitter.util.Time

private class TransporterServiceFactory(
  transporter: Transporter[Any, Any, TransportContext],
  params: Stack.Params)
    extends ServiceFactory[Request, Response] {

  private[this] val modifierFn = params[TransportModifier].modifier
  private[this] val dispatcherStats =
    params[Stats].statsReceiver.scope(ClientDispatcher.StatsScope)

  def apply(conn: ClientConnection): Future[Service[Request, Response]] =
    // we do not want to capture and request specific Locals
    // that would live for the life of the session.
    Contexts.letClearAll {
      transporter().map { trans =>
        val streamTransport = new Netty4ClientStreamTransport(modifierFn(trans))

        val httpTransport = trans match {
          case _: StreamChannelTransport => new Http2Transport(streamTransport)
          case _ => new HttpTransport(streamTransport)
        }

        new HttpClientDispatcher(
          httpTransport,
          dispatcherStats
        )
      }
    }

  def close(deadline: Time): Future[Unit] = Future.Done

  def status: finagle.Status = finagle.Status.Open
}
