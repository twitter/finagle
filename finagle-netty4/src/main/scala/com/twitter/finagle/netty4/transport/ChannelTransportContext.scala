package com.twitter.finagle.netty4.transport

import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo, UsingSslSessionInfo}
import com.twitter.finagle.transport.TransportContext
import io.netty.channel.Channel
import io.netty.handler.ssl.SslHandler
import java.net.SocketAddress
import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 * `TransportContext` for use with a Finagle Netty4
 * `ChannelTransport`.
 */
private[finagle] final class ChannelTransportContext(val ch: Channel) extends TransportContext {

  def localAddress: SocketAddress = ch.localAddress

  def remoteAddress: SocketAddress = ch.remoteAddress

  private[this] def getSslHandler(ch: Channel): SslHandler =
    ch.pipeline.get(classOf[SslHandler])

  // We extract the `SslHandler` from this `Channel` if it's available.
  // If not, we try the channel's parent instead. Looking at the parent
  // is necessary when a request is processed by a child channel and
  // pipeline (i.e. HTTP/2).
  @tailrec
  private[this] def getSslSessionInfo(ch: Channel): SslSessionInfo =
    getSslHandler(ch) match {
      case null =>
        if (ch.parent != null) getSslSessionInfo(ch.parent)
        else NullSslSessionInfo
      case handler =>
        try {
          new UsingSslSessionInfo(handler.engine.getSession)
        } catch {
          case NonFatal(_) => NullSslSessionInfo
        }
    }

  val sslSessionInfo: SslSessionInfo = getSslSessionInfo(ch)
}
