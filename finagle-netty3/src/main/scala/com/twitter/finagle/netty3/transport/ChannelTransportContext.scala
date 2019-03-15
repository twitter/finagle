package com.twitter.finagle.netty3.transport

import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo, UsingSslSessionInfo}
import com.twitter.finagle.transport.TransportContext
import java.net.SocketAddress
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

  def sslSessionInfo: SslSessionInfo =
    ch.getPipeline.get(classOf[SslHandler]) match {
      case null => NullSslSessionInfo
      case handler =>
        try {
          new UsingSslSessionInfo(handler.getEngine.getSession)
        } catch {
          case NonFatal(_) => NullSslSessionInfo
        }
    }

}
