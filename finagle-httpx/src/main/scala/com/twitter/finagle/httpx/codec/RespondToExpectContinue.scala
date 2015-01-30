package com.twitter.finagle.httpx.codec

/**
 * A simple channel handler to respond to "Expect: Continue" from
 * clients. It responds unconditionally to these.
 */

import org.jboss.netty.channel.{
  SimpleChannelUpstreamHandler, Channels,
  ChannelHandlerContext, MessageEvent}
import org.jboss.netty.handler.codec.http.{HttpHeaders, HttpRequest=>HttpAsk}

private[httpx]
class RespondToExpectContinue extends SimpleChannelUpstreamHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case request: HttpAsk if HttpHeaders.is100ContinueExpected(request) =>
        // Write the response immediately.
        Channels.write(
          ctx, Channels.future(ctx.getChannel),
          OneHundredContinueResponse, e.getRemoteAddress)

        // Remove the the ``Expect:'' header, and let the upstream
        // continue receiving chunks after this.
        request.headers.remove(HttpHeaders.Names.EXPECT)

      case _ => ()
    }

    super.messageReceived(ctx, e)
  }
}
