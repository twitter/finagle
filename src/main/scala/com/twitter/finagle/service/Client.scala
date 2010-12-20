package com.twitter.finagle.service

import org.jboss.netty.channel.{Channels, MessageEvent}

import com.twitter.finagle.channel._
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._

import com.twitter.util.{Future, Promise, Return, Throw}

class ReplyIsStreamingException   extends Exception
class CancelledRequestException   extends Exception
class InvalidMessageTypeException extends Exception

class Client[-Req <: AnyRef, +Rep <: AnyRef](broker: Broker)
  extends Service[Req, Rep]
{
  def apply(request: Req): Future[Rep] = {
    val messageEvent = new MessageEvent {
      val future = Channels.future(null)

      def getMessage       = request
      def getFuture        = future
      def getRemoteAddress = null
      def getChannel       = null
    }

    val replyFuture = broker.dispatch(messageEvent)
    val promise = new Promise[Rep]

    replyFuture {
      case Ok(_) =>
        replyFuture.getReply match {
          case Reply.Done(reply) =>
            if (reply.isInstanceOf[Rep])
              promise() = Return(reply.asInstanceOf[Rep])
            else
              promise() = Throw(new InvalidMessageTypeException)

          // case Reply.Done(reply: Rep) =>
          //   promise() = Return(reply)
          // case Reply.Done(_) =>
          //   promise() = Throw(new InvalidMessageTypeException)
          case Reply.More(_, _) =>
            promise() = Throw(new ReplyIsStreamingException)
        }

      case Error(cause) =>
        promise() = Throw(cause)

      case _ =>
        promise() = Throw(new CancelledRequestException)
    }

    promise
  }
}

