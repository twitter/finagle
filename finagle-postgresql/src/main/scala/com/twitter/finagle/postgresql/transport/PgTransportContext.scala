package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.finagle.transport.TransportContext
import java.net.SocketAddress

final class PgTransportContext(
  val connectionParameters: Response.ConnectionParameters,
  wrapped: TransportContext)
    extends TransportContext {
  override def localAddress: SocketAddress = wrapped.localAddress
  override def remoteAddress: SocketAddress = wrapped.remoteAddress
  override def sslSessionInfo: SslSessionInfo = wrapped.sslSessionInfo
}
