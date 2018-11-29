package com.twitter.finagle.transport

import com.twitter.util.Updatable
import java.net.SocketAddress
import java.security.cert.Certificate

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
   * The peer certificate if a TLS session is established.
   */
  def peerCertificate: Option[Certificate]
}

/**
 * A TransportContext that can derive its methods from an underlying transport.
 *
 * Useful as a stopgap before implementing the methods on TransportContext
 * directly.
 */
private[finagle] class LegacyContext(underlying: Transport[_, _]) extends TransportContext {
  def localAddress: SocketAddress = underlying.localAddress
  def remoteAddress: SocketAddress = underlying.remoteAddress
  def peerCertificate: Option[Certificate] = underlying.peerCertificate
}

private[finagle] class UpdatableContext(first: TransportContext)
    extends TransportContext
    with Updatable[TransportContext] {
  @volatile private[this] var underlying: TransportContext = first

  def update(context: TransportContext): Unit = {
    underlying = context
  }

  def localAddress: SocketAddress = underlying.localAddress
  def remoteAddress: SocketAddress = underlying.remoteAddress
  def peerCertificate: Option[Certificate] = underlying.peerCertificate
}
