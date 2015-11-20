package com.twitter.finagle.netty4.ssl

import java.lang.reflect.Method
import java.util.logging.Logger
import javax.net.ssl.SSLException
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

private[netty4] class TlsShutdownHandler(o: Object) extends ChannelInboundHandlerAdapter {
  private[this] val shutdownMethod: Option[Method] =
    try {
      Some(o.getClass.getMethod("shutdown"))
    } catch {
      case _: NoSuchMethodException => None
    }

  private[this] def shutdownAfterChannelClosure(): Unit =
    shutdownMethod.foreach { method =>
      method.invoke(o)
    }

  override def exceptionCaught(ctx: ChannelHandlerContext, t: Throwable): Unit = {
    // remove the ssl handler so that it doesn't trap the disconnect
    if (t.isInstanceOf[SSLException])
      ctx.pipeline().remove("ssl")

    super.exceptionCaught(ctx, t)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    shutdownAfterChannelClosure()
    super.channelInactive(ctx)
  }
}
