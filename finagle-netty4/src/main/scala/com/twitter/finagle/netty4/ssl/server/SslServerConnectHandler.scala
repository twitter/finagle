package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}
import javax.net.ssl.SSLSession
import scala.util.control.NonFatal

/**
 * Delays `channelRegistered` event until the TLS handshake is successfully finished.
 */
private[netty4] class SslServerConnectHandler(
  sslHandler: SslHandler,
  config: SslServerConfiguration,
  sessionVerifier: SslServerSessionVerifier
) extends ChannelInboundHandlerAdapter { self =>

  private[this] def verifySession(ctx: ChannelHandlerContext, session: SSLSession): Unit = {
    try {
      if (sessionVerifier(config, session)) {
        ctx.pipeline().remove(self)
        ctx.fireChannelActive()
      } else {
        ctx.close()
      }
    } catch {
      case NonFatal(e) => ctx.close()
    }
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    sslHandler
      .handshakeFuture()
      .addListener(new GenericFutureListener[NettyFuture[Channel]] {
        override def operationComplete(f: NettyFuture[Channel]): Unit = {
          if (f.isSuccess) {
            val session = sslHandler.engine().getSession
            verifySession(ctx, session)
          }
        }
      })

    if (!ctx.channel().config().isAutoRead) {
      ctx.read()
    }
  }

}
