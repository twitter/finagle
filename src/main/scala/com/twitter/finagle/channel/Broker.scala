package com.twitter.finagle.channel

import java.net.SocketAddress
import org.jboss.netty.channel.{
  Channel, ChannelFuture, MessageEvent,
  ChannelLocal, DefaultChannelFuture}

sealed abstract class Reply
object Reply {
  case class Done(message: AnyRef) extends Reply
  case class More(message: AnyRef, next: ReplyFuture) extends Reply
}

class ReplyFuture extends DefaultChannelFuture(null, true) {
  @volatile var reply: Reply = null

  def setReply(r: Reply) {
    reply = r
    setSuccess()
  }

  def getReply = reply
}

object ReplyFuture {
  def success(message: AnyRef) = {
    val r = new ReplyFuture
    r.setReply(Reply.Done(message))
    r
  }

  def failed(cause: Throwable) = {
    val r = new ReplyFuture
    r.setFailure(cause)
    r
  }
}

trait Broker extends SocketAddress {
  def dispatch(e: MessageEvent): ReplyFuture
}
