package com.twitter.finagle.netty4.transport

import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo, UsingSslSessionInfo}
import com.twitter.finagle.transport.TransportContext
import io.netty.channel.Channel
import io.netty.handler.ssl.SslHandler
import java.net.SocketAddress
import java.util.concurrent.Executor
import scala.util.control.NonFatal

/**
 * `TransportContext` for use with a Finagle Netty4
 * `ChannelTransport`.
 */
private[finagle] final class ChannelTransportContext(val ch: Channel)
    extends TransportContext
    with HasExecutor {

  def localAddress: SocketAddress = ch.localAddress

  def remoteAddress: SocketAddress = ch.remoteAddress

  val sslSessionInfo: SslSessionInfo =
    ch.pipeline.get(classOf[SslHandler]) match {
      case null => NullSslSessionInfo
      case handler =>
        try {
          new UsingSslSessionInfo(handler.engine.getSession)
        } catch {
          case NonFatal(_) => NullSslSessionInfo
        }
    }

  def executor: Executor = ch.eventLoop
}
