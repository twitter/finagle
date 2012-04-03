package com.twitter.finagle.http.codec

import com.twitter.finagle.http.Request
import com.twitter.logging.Logger
import org.jboss.netty.channel.{ChannelHandlerContext, MessageEvent, SimpleChannelDownstreamHandler}
import org.jboss.netty.handler.codec.http.HttpHeaders

/**
 * Convert Finagle-HTTP requests to Netty Requests
 */
class RequestEncoder extends SimpleChannelDownstreamHandler {
  private[this] val log = Logger("finagle-http")

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case request: Request =>
        assert(!request.isChunked)
        if (!request.containsHeader(HttpHeaders.Names.CONTENT_LENGTH))
          request.contentLength = request.getContent().readableBytes
        super.writeRequested(ctx, e)

      case unknown =>
        log.warning("RequestEncoder: illegal message type: %s", unknown)
        super.writeRequested(ctx, e)
    }
  }
}
