package com.twitter.finagle.http.codec

import org.jboss.netty.channel._

import com.twitter.finagle.channel.ChannelServiceReply

private[finagle] class ClientConnectionManager extends SimpleChannelHandler {
  private[this] val manager = new ConnectionManager

  // Note that for HTTP requests without a content length, the
  // underlying codec does the right thing (flushes the buffer until
  // the connection has been closed).
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    manager.observeMessage(e.getMessage)
    val reply = ChannelServiceReply(e.getMessage, manager.shouldClose)
    val wrappedMessageEvent =
      new UpstreamMessageEvent(e.getChannel, reply, e.getRemoteAddress)
    super.messageReceived(ctx, wrappedMessageEvent)
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    manager.observeMessage(e.getMessage)
    super.writeRequested(ctx, e)
  }
}
