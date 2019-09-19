package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.netty4.http.Netty4ClientStreamTransport
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, Status}
import com.twitter.util.{Future, Return, Throw, Time}

/**
 * Concurrent `Service` HTTP/2 implementation.
 */
private final class ClientServiceImpl(
  session: ClientSession,
  stats: StatsReceiver,
  modifier: Transport[Any, Any] => Transport[Any, Any])
    extends Service[Request, Response] {

  override def status: Status = session.status

  def apply(request: Request): Future[Response] =
    session.newChildTransport().transform {
      case Return(trans) =>
        val svc = mkService(trans)
        svc(request)

      case t @ Throw(_) =>
        Future.const(t.cast[Response])
    }

  private[this] def mkService(trans: Transport[Any, Any]): Service[Request, Response] = {
    val streamTransport = new Netty4ClientStreamTransport(modifier(trans))
    val httpTransport = new Http2Transport(streamTransport)
    new Http2ClientDispatcher(
      httpTransport,
      stats
    )
  }

  override def close(deadline: Time): Future[Unit] =
    session.close(deadline)

  override def toString: String = s"${getClass.getSimpleName}($session)"
}
