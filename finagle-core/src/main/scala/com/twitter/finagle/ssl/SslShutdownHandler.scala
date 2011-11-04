package com.twitter.finagle.ssl

import java.lang.reflect.Method
import java.util.logging.Logger

import org.jboss.netty.channel.{
  ChannelHandlerContext, ChannelStateEvent, SimpleChannelUpstreamHandler
}

class SslShutdownHandler(o: Object) extends SimpleChannelUpstreamHandler {
  private[this] val log = Logger.getLogger(getClass().getName())
  private[this] val shutdownMethod: Option[Method] =
    try {
      Some(o.getClass().getMethod("shutdown"))
    } catch {
      case _: NoSuchMethodException => None
    }

  private[this] def shutdownAfterChannelClosure() {
    shutdownMethod foreach { method: Method =>
      method.invoke(o)
    }
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    shutdownAfterChannelClosure()

    super.channelClosed(ctx, e)
  }
}
