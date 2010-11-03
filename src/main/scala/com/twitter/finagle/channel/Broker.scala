package com.twitter.finagle.channel

import java.util.concurrent.TimeUnit
import java.net.SocketAddress

import org.jboss.netty.channel.{
  Channel, ChannelFuture, MessageEvent,
  ChannelLocal, DefaultChannelFuture}
import org.jboss.netty.util.HashedWheelTimer

import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._

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

  def whenDone(f: => Unit): ReplyFuture = whenDone0 { () => f }
  def whenDone0(f: Function0[Unit]) = {
    this onSuccessOrFailure {
      getReply match {
        case Reply.More(_, next) =>
          next.whenDone(f)
        case _ =>
          f()
      }
    }
    this
  }
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

object Broker {
  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
}
