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

private[finagle] class SimpleTransportContext(
  val localAddress: SocketAddress = new SocketAddress {},
  val remoteAddress: SocketAddress = new SocketAddress {},
  val peerCertificate: Option[Certificate] = None)
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
  def peerCertificate: Option[Certificate] = underlying.peerCertificate
}
