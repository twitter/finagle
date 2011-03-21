package com.twitter.finagle.http

import org.jboss.netty.channel._

private[finagle] class ClientConnectionManager extends SimpleChannelHandler {
  private[this] val manager = new ConnectionManager

  // Note that for HTTP requests without a content length, the
  // underlying codec does the right thing (flushes the buffer until
  // the connection has been closed).
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    super.messageReceived(ctx, e)

    manager.observeMessage(e.getMessage)
    if (manager.shouldClose)
      e.getChannel.close()
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    manager.observeMessage(e.getMessage)
    super.writeRequested(ctx, e)
  }
}
