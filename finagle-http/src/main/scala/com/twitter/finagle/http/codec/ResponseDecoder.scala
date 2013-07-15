package com.twitter.finagle.http.codec

import com.twitter.finagle.http.Response
import com.twitter.logging.Logger
import org.jboss.netty.channel.{Channels, ChannelHandlerContext, MessageEvent, SimpleChannelHandler}
import org.jboss.netty.handler.codec.http.HttpResponse

/**
 * Convert Netty responses to a Finagle-HTTP responses
 */
class ResponseDecoder extends SimpleChannelHandler {
  private[this] val log = Logger("finagle-http")

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case finagleResponse: Response =>
        Channels.fireMessageReceived(ctx, finagleResponse)

      case httpResponse: HttpResponse =>
        Channels.fireMessageReceived(ctx, Response(httpResponse))

      case unknown =>
        log.warning("ClientRequestDecoder: illegal message type: %s", unknown.getClass())
        Channels.disconnect(ctx.getChannel)
    }
  }
}
