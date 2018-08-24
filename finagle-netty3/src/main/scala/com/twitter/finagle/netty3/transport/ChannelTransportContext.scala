package com.twitter.finagle.netty3.transport

import com.twitter.finagle.Status
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.{Future, Promise, Time}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.channel.{Channel, Channels}
import org.jboss.netty.handler.ssl.SslHandler
import scala.util.control.NonFatal

/**
 * `TransportContext` for use with a Finagle Netty 3
 * `ChannelTransport`.
 */
final class ChannelTransportContext private[transport](ch: Channel) extends TransportContext {
  // accessible by the ChannelTransport and for testing
  private[transport] val failed = new AtomicBoolean(false)
  private[transport] val closep = new Promise[Throwable]

  def status: Status =
    if (failed.get || !ch.isOpen) Status.Closed
    else Status.Open

  def onClose: Future[Throwable] = closep

  def localAddress: SocketAddress = ch.getLocalAddress()
  def remoteAddress: SocketAddress = ch.getRemoteAddress()

  def peerCertificate: Option[Certificate] =
    ch.getPipeline.get(classOf[SslHandler]) match {
      case null => None
      case handler =>
        try {
          handler.getEngine.getSession.getPeerCertificates.headOption
        } catch {
          case NonFatal(_) => None
        }
    }

  def close(deadline: Time): Future[Unit] = {
    if (ch.isOpen)
      Channels.close(ch)
    closep.unit
  }

}
