package com.twitter.finagle.netty4.transport

import com.twitter.finagle.transport.TransportContext
import io.netty.channel.Channel
import io.netty.handler.ssl.SslHandler
import java.net.SocketAddress
import java.security.cert.Certificate
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

  val peerCertificate: Option[Certificate] = ch.pipeline.get(classOf[SslHandler]) match {
    case null => None
    case handler =>
      try {
        handler.engine.getSession.getPeerCertificates.headOption
      } catch {
        case NonFatal(_) => None
      }
  }

  def executor: Executor = ch.eventLoop
}
