package com.twitter.finagle.transport

import com.twitter.finagle.Status
import com.twitter.util.{Future, Closable, Time, Updatable}
import java.net.SocketAddress
import java.security.cert.Certificate

/**
 * Exposes a way to control the transport, and read off properties from the
 * transport.
 */
abstract class TransportContext extends Closable {

  /**
   * The status of this transport; see [[com.twitter.finagle.Status]] for
   * status definitions.
   */
  def status: Status

  /**
   * The channel closed with the given exception. This is the
   * same exception you would get if attempting to read or
   * write on the Transport, but this allows clients to listen to
   * close events.
   */
  def onClose: Future[Throwable]

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
  def status: Status = underlying.status
  def close(deadline: Time): Future[Unit] = underlying.close(deadline)
  def onClose: Future[Throwable] = underlying.onClose
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

  def status: Status = underlying.status
  def close(deadline: Time): Future[Unit] = underlying.close(deadline)
  def onClose: Future[Throwable] = underlying.onClose
  def localAddress: SocketAddress = underlying.localAddress
  def remoteAddress: SocketAddress = underlying.remoteAddress
  def peerCertificate: Option[Certificate] = underlying.peerCertificate
}
