package com.twitter.finagle.netty3.transport

import com.twitter.finagle.transport.TransportContext
import java.net.SocketAddress
import java.security.cert.Certificate
import org.jboss.netty.channel.Channel
import org.jboss.netty.handler.ssl.SslHandler
import scala.util.control.NonFatal

/**
 * `TransportContext` for use with a Finagle Netty 3
 * `ChannelTransport`.
 */
final class ChannelTransportContext private[transport] (ch: Channel) extends TransportContext {

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

}
