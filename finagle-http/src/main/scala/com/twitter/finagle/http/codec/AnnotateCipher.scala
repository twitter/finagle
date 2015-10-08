package com.twitter.finagle.http.codec

import org.jboss.netty.channel.{ChannelHandlerContext, MessageEvent, SimpleChannelHandler}
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.ssl.SslHandler

/**
 * Extract the cipher from the SslHandler and set it as a header on the HTTP
 * request befor sending it upstream.
 */
private[http]
class AnnotateCipher(headerName: String) extends SimpleChannelHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    (e.getMessage, ctx.getPipeline.get(classOf[SslHandler])) match {
      case (req: HttpRequest, ssl: SslHandler) =>
        req.headers.set(headerName, ssl.getEngine().getSession().getCipherSuite())
      case _ =>
        ()
    }

    super.messageReceived(ctx, e)
  }
}
