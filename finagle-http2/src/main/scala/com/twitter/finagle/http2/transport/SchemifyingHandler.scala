package com.twitter.finagle.http2.transport

import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http2.HttpConversionUtil.ExtensionHeaderNames.SCHEME

/**
 * Http/2 requires that clients specify a scheme, and netty wants to get that
 * from a port, prefix, or special header.  Most use cases don't need a prefix
 * or a port, so we add the special header on behalf of users.
 */
private[http2] class SchemifyingHandler(defaultScheme: String) extends ChannelDuplexHandler {
  override def userEventTriggered(ctx: ChannelHandlerContext, event: Any): Unit = {
    event match {
      case _ @UpgradeEvent.UPGRADE_REJECTED =>
        // we no longer need to schemify if the upgrade was rejected
        ctx.pipeline.remove(this)
      case _ => // nop
    }
    super.userEventTriggered(ctx, event)
  }

  /**
   * We need to make sure that we add the scheme properly, since it'll be rejected
   * by the http2 codec if we don't.
   */
  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
    msg match {
      case req: HttpRequest =>
        if (!req.headers.contains(SCHEME.text(), defaultScheme, true /*ignoreCase*/ ))
          req.headers.add(SCHEME.text(), defaultScheme)
      case _ => // nop
    }
    ctx.write(msg, promise)
  }
}

private[http2] object SchemifyingHandler {
  val HandlerName =  "schemifier"
}
