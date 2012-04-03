package com.twitter.finagle.http.codec

import com.twitter.finagle.channel.ChannelServiceReply
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

      // todo: this case will go away once Marius makes the change to ChannelServiceReply.
      case ChannelServiceReply(httpResponse: HttpResponse, close) =>
        Channels.fireMessageReceived(ctx, ChannelServiceReply(Response(httpResponse), close))

      case unknown =>
        log.warning("ClientRequestDecoder: illegal message type: %s", unknown.getClass())
        Channels.disconnect(ctx.getChannel)
    }
  }
}