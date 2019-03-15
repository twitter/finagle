package com.twitter.finagle.transport

import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo}
import com.twitter.util.Updatable
import java.net.SocketAddress

/**
 * Exposes a way to control the transport, and read off properties from the
 * transport.
 */
abstract class TransportContext {

  /**
   * The locally bound address of this transport.
   */
  def localAddress: SocketAddress

  /**
   * The remote address to which the transport is connected.
   */
  def remoteAddress: SocketAddress

  /**
   * SSL/TLS session information associated with the transport.
   *
   * @note If SSL/TLS is not being used a `NullSslSessionInfo` will be returned instead.
   */
  def sslSessionInfo: SslSessionInfo
}

private[finagle] class SimpleTransportContext(
  val localAddress: SocketAddress = new SocketAddress {},
  val remoteAddress: SocketAddress = new SocketAddress {},
  val sslSessionInfo: SslSessionInfo = NullSslSessionInfo)
    extends TransportContext

private[finagle] class UpdatableContext(first: TransportContext)
    extends TransportContext
    with Updatable[TransportContext] {
  @volatile private[this] var underlying: TransportContext = first

  def update(context: TransportContext): Unit = {
    underlying = context
  }

  def localAddress: SocketAddress = underlying.localAddress
  def remoteAddress: SocketAddress = underlying.remoteAddress
  def sslSessionInfo: SslSessionInfo = underlying.sslSessionInfo
}
