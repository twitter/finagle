package com.twitter.finagle.server

import com.twitter.finagle.ClientConnection
import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.{Future, Time}
import java.net.SocketAddress

/**
 * Class which adapts a `Transport` on the server side to the
 * interface of a `ClientConnection`. It relies on methods within
 * the `Transport` and the associated `TransportContext`.
 *
 * @param t An established `Transport` with the client which may
 * or may not be open.
 */
private[finagle] class TransportClientConnection[In, Out](
  t: Transport[In, Out] {
    type Context <: TransportContext
  }) extends ClientConnection {

  override def remoteAddress: SocketAddress = t.context.remoteAddress
  override def localAddress: SocketAddress = t.context.localAddress
  // In the Transport + Dispatcher model, the Transport is a source of truth for
  // the `onClose` future: closing the dispatcher will result in closing the
  // Transport and closing the Transport will trigger shutdown of the dispatcher.
  // Therefore, even when we swap the closable that is the target of `this.close(..)`,
  // they both will complete the transports `onClose` future.
  override val onClose: Future[Unit] = t.onClose.unit
  override def close(deadline: Time): Future[Unit] = t.close(deadline)
  override def sslSessionInfo: SslSessionInfo = t.context.sslSessionInfo
}
