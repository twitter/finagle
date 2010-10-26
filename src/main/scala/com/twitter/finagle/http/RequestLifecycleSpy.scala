package com.twitter.finagle.http

import org.jboss.netty.channel.{
  SimpleChannelUpstreamHandler, ChannelHandlerContext, MessageEvent}
import org.jboss.netty.handler.codec.http.{
  HttpChunkTrailer, HttpResponse, HttpChunk}

import com.twitter.finagle.channel.PartialUpstreamMessageEvent

object RequestLifecycleSpy extends SimpleChannelUpstreamHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val upstreamMessage = 
      e.getMessage match {
        case response: HttpResponse if response.isChunked =>
          PartialUpstreamMessageEvent(e)
        case _: HttpChunkTrailer => e
        case c: HttpChunk =>
          PartialUpstreamMessageEvent(e)
        case _ => e
      }

    super.messageReceived(ctx, upstreamMessage)
  }
}
