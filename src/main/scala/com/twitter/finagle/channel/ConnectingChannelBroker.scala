package com.twitter.finagle.channel

import java.nio.channels.ClosedChannelException

import org.jboss.netty.channel.{
  Channels, Channel, MessageEvent, SimpleChannelUpstreamHandler,
  ChannelHandlerContext, ChannelStateEvent, ExceptionEvent,
  ChannelFuture}

import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._

trait ConnectingChannelBroker extends Broker {
  def getChannel: ChannelFuture
  def putChannel(channel: Channel)

  def dispatch(e: MessageEvent) = {
    val replyFuture = new ReplyFuture

    getChannel {
      case Ok(channel) if replyFuture.isCancelled =>
        putChannel(channel)

      case Ok(channel) =>
        connectChannel(channel, e, replyFuture)
        replyFuture whenDone {
          putChannel(channel)
        }

      case Error(cause) =>
        replyFuture.setFailure(cause)

      case Cancelled => ()
    }

    replyFuture
  }

  protected def connectChannel(to: Channel, e: MessageEvent, replyFuture: ReplyFuture) {
    val handler = new ChannelConnectingHandler(replyFuture, to)
    to.getPipeline.addLast("handler", handler)
    Channels.write(to, e.getMessage).proxyTo(e.getFuture)
  }

  private class ChannelConnectingHandler(
    firstReplyFuture: ReplyFuture,
    to: Channel)
    extends SimpleChannelUpstreamHandler
  {
    var replyFuture = firstReplyFuture

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      e match {
        case PartialUpstreamMessageEvent(_, message, _) =>
          val next = new ReplyFuture
          replyFuture.setReply(Reply.More(message, next))
          replyFuture = next
        case _ =>
          replyFuture.setReply(Reply.Done(e.getMessage))
          to.getPipeline.remove(this)
      }
    }

    // We rely on the underlying protocol handlers to close channels
    // on errors. (This is probably the only sane policy).
    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      replyFuture.setFailure(new ClosedChannelException)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, exc: ExceptionEvent) {
      replyFuture.setFailure(exc.getCause)
    }
  }
}
