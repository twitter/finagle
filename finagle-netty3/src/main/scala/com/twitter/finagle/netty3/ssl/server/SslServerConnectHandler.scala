package com.twitter.finagle.netty3.ssl.server

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import java.net.InetSocketAddress
import javax.net.ssl.SSLException
import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import scala.util.control.NonFatal

/**
 * Handle server-side SSL Connections:
 *
 * 1. by delaying the upstream connect until the SSL handshake
 *    is complete (so that we don't send data through a connection
 *    we may later deem invalid), and
 * 2. invoking a shutdown callback on disconnect
 */
private[netty3] class SslServerConnectHandler(
  sslHandler: SslHandler,
  config: SslServerConfiguration,
  sessionVerifier: SslServerSessionVerifier,
  onShutdown: () => Unit = () => Unit
) extends SimpleChannelUpstreamHandler {

  // delay propagating connection upstream until we've completed the handshake
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    sslHandler
      .handshake()
      .addListener(new ChannelFutureListener {
        override def operationComplete(f: ChannelFuture): Unit = {
          val remoteAddress: Address =
            // guard against disconnected sessions and test environments with mock channels
            if (ctx.getChannel.getRemoteAddress == null || !ctx.getChannel.getRemoteAddress.isInstanceOf[InetSocketAddress])
              Address.failing
            else Address(ctx.getChannel.getRemoteAddress.asInstanceOf[InetSocketAddress])

          if (f.isSuccess) {
            try {
              if (sessionVerifier(remoteAddress, config, sslHandler.getEngine.getSession)) {
                SslServerConnectHandler.super.channelConnected(ctx, e)
              } else {
                Channels.close(ctx.getChannel)
              }
            } catch {
              case NonFatal(_) => Channels.close(ctx.getChannel)
            }
          } else {
            Channels.close(ctx.getChannel)
          }
        }
      })
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
    // remove the ssl handler so that it doesn't trap the disconnect
    if (e.getCause.isInstanceOf[SSLException])
      ctx.getPipeline.remove("ssl")
    super.exceptionCaught(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    onShutdown()
    super.channelClosed(ctx, e)
  }
}
