package com.twitter.finagle.httpx.codec

import com.twitter.finagle.httpx.Response
import com.twitter.finagle.httpx.netty.Bijections
import com.twitter.logging.Logger
import org.jboss.netty.channel.{Channels, ChannelHandlerContext, MessageEvent, SimpleChannelHandler}
import org.jboss.netty.handler.codec.http.HttpResponse

import Bijections._

/**
 * Convert Netty responses to a Finagle-HTTP responses
 */
class ResponseDecoder extends SimpleChannelHandler {
  private[this] val log = Logger("finagle.httpx")

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case finagleResponse: Response =>
        Channels.fireMessageReceived(ctx, finagleResponse)

      case httpResponse: HttpResponse =>
        Channels.fireMessageReceived(ctx, from(httpResponse))

      case unknown =>
        log.warning("ClientRequestDecoder: illegal message type: %s", unknown.getClass())
        Channels.disconnect(ctx.getChannel)
    }
  }
}
