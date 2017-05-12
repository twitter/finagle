package com.twitter.finagle.http.netty

import com.twitter.finagle.http.{BadRequestResponse, Response}
import org.jboss.netty.channel.{Channel, ChannelFutureListener, Channels, DownstreamMessageEvent}
import org.jboss.netty.handler.codec.frame.TooLongFrameException

/**
 * Tool for converting netty3 HttpServerCodec errors to meaningful responses.
 */
private[http] object BadMessageConverter {
  /**
   * Convert exceptions into a HTTP response with an appropriate status code. The `ChannelFuture`
   * associated with the resulting `DownstreamMessageEvent` will close the `Channel` after
   * the event is handled.
   */
  def errorToDownstreamEvent(ch: Channel, ex: Exception): DownstreamMessageEvent = {
    val resp = makeResponse(ex)
    val f = Channels.future(ch)
    f.addListener(ChannelFutureListener.CLOSE)

    new DownstreamMessageEvent(ch, f, Bijections.responseToNetty(resp), ch.getRemoteAddress)
  }

  private def makeResponse(ex: Exception): Response = ex match {
    case ex: TooLongFrameException =>
      if (ex.getMessage().startsWith("An HTTP line is larger than "))
        BadRequestResponse.uriTooLong()
      else if (ex.getMessage().startsWith("HTTP content length exceeded "))
        BadRequestResponse.contentTooLong()
      else
        BadRequestResponse.headerTooLong()
    case _ =>
      BadRequestResponse()
  }
}
