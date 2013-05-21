package com.twitter.finagle.http.codec

import com.twitter.finagle.http.Response
import org.jboss.netty.channel.{ChannelHandlerContext, MessageEvent, SimpleChannelDownstreamHandler}
import org.jboss.netty.handler.codec.http.HttpHeaders


/**
 * Convert Finagle-HTTP Responses to Netty responses
 */
class ResponseEncoder extends SimpleChannelDownstreamHandler {

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case response: Response =>
        // Ensure Content-Length is set
        if (!response.containsHeader(HttpHeaders.Names.CONTENT_LENGTH))
          response.contentLength = response.getContent().readableBytes
        super.writeRequested(ctx, e)
      case _ =>
        super.writeRequested(ctx, e)
    }
  }
}
