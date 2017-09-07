package com.twitter.finagle.netty3.http

import com.twitter.finagle.http
import java.net.SocketAddress
import org.jboss.netty.channel.ChannelHandler.Sharable
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.TooLongFrameException

/**
 * Translate Netty 3 http `Exception`s to Finagle `ChannelException`s
 */
@Sharable
private object ClientExceptionMapper extends SimpleChannelUpstreamHandler {
  override def exceptionCaught(
    ctx: ChannelHandlerContext,
    e: ExceptionEvent
  ): Unit = {
    val newEvent = translateExceptionEvent(ctx.getChannel.getRemoteAddress, e)
    super.exceptionCaught(ctx, newEvent)
  }

  private def translateExceptionEvent(
    remote: SocketAddress,
    event: ExceptionEvent
  ): ExceptionEvent =
    event.getCause match {
      case e: TooLongFrameException =>
        makeEvent(event, http.TooLongMessageException(e, remote))
      case _ => event
    }

  private def makeEvent(old: ExceptionEvent, e: Exception): ExceptionEvent =
    new DefaultExceptionEvent(old.getChannel, e)
}
