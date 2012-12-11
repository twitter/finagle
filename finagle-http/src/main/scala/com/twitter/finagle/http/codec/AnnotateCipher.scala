package com.twitter.finagle.http.codec

import org.jboss.netty.channel.{Channel, ChannelHandler, ChannelHandlerContext,
                                ChannelStateEvent, MessageEvent, SimpleChannelHandler}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.jboss.netty.handler.ssl.SslHandler

import com.twitter.finagle.netty3.{Ok, Error}
import com.twitter.finagle.netty3.Conversions._

/**
 * Extract the cipher from the SslHandler and set it as a header on the HTTP
 * request befor sending it upstream.
 */
class AnnotateCipher(headerName: String) extends SimpleChannelHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    (e.getMessage, ctx.getPipeline.get(classOf[SslHandler])) match {
      case (req: HttpRequest, ssl: SslHandler) =>
        req.setHeader(headerName, ssl.getEngine().getSession().getCipherSuite())
      case _ =>
        ()
    }

    super.messageReceived(ctx, e)
  }
}
