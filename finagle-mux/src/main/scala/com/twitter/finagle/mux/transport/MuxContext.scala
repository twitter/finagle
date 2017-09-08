package com.twitter.finagle.mux.transport

import com.twitter.finagle.Status
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.{Future, Time}
import com.twitter.logging.Logger
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicBoolean

private[finagle] class MuxContext(
  underlying: TransportContext,
  turnOnTlsFn: () => Unit
) extends TransportContext {
  def status: Status = underlying.status
  def close(deadline: Time): Future[Unit] = underlying.close(deadline)
  def onClose: Future[Throwable] = underlying.onClose
  def localAddress: SocketAddress = underlying.localAddress
  def remoteAddress: SocketAddress = underlying.remoteAddress
  def peerCertificate: Option[Certificate] = underlying.peerCertificate

  private[this] val log = Logger.get()
  private[this] val turnedOn = new AtomicBoolean(false)

  /**
   * Turns on TLS.
   */
  def turnOnTls(): Unit = {
    if (turnedOn.compareAndSet(false, true)) {
      turnOnTlsFn()
    } else {
      log.warning("Tried to enable tls more than once, which probably isn't what you want to do")
    }
  }
}
