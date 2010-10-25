package com.twitter.finagle.http

import org.jboss.netty.channel.{
  SimpleChannelUpstreamHandler, ChannelHandlerContext, MessageEvent}
import org.jboss.netty.handler.codec.http.{
  HttpChunkTrailer, HttpResponse, HttpChunk}

import com.twitter.finagle.channel.Broker

object RequestLifecycleSpy extends SimpleChannelUpstreamHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case response: HttpResponse if response.isChunked =>
        Broker.setChannelBusy(ctx.getChannel)
      case response: HttpResponse if !response.isChunked =>
        Broker.setChannelIdle(ctx.getChannel)
      case response: HttpChunkTrailer =>
        Broker.setChannelIdle(ctx.getChannel)
      case _ =>
    }

    super.messageReceived(ctx, e)
  }
}
