package com.twitter.finagle.http

import com.twitter.finagle.builder.SslCipherAttribution

import org.jboss.netty.channel.{Channel, ChannelHandler, ChannelHandlerContext, MessageEvent,
                                SimpleChannelHandler}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

/**
 * Extract the cipher from the SslCipherAttribution ChannelLocal variable and
 * set it as a header on the HTTP request befor sending it upstream.
 */
class AnnotateCipher extends SimpleChannelHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    if (e.getMessage.isInstanceOf[HttpRequest]) {
      val req = e.getMessage.asInstanceOf[HttpRequest]
      val cipher = SslCipherAttribution(ctx.getChannel)
      req.setHeader("X-Transport-Cipher", cipher)
    }

    super.messageReceived(ctx, e)
  }
}
