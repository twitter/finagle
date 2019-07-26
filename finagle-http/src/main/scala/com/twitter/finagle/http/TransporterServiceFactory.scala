package com.twitter.finagle.http

import com.twitter.finagle.{
  ClientConnection,
  Service,
  ServiceFactory,
  Stack,
  Status => FinagleStatus
}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.dispatch.ClientDispatcher
import com.twitter.finagle.http.ClientEndpointer.TransportModifier
import com.twitter.finagle.http.codec.HttpClientDispatcher
import com.twitter.finagle.http2.exp.transport.{Http2Transport, StreamChannelTransport}
import com.twitter.finagle.http2.transport.MultiplexTransporter
import com.twitter.finagle.netty4.http.Netty4ClientStreamTransport
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.{Future, Time}

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

  def close(deadline: Time): Future[Unit] = transporter match {
    case multiplex: MultiplexTransporter => multiplex.close(deadline)
    case _ => Future.Done
  }

  override def status: FinagleStatus = transporter match {
    case http2: MultiplexTransporter => http2.transporterStatus
    case _ => super.status
  }
}
