package com.twitter.finagle.stub

import org.jboss.netty.channel.{Channels, MessageEvent}

import com.twitter.finagle.channel._
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import com.twitter.finagle.util.Conversions._

import com.twitter.util.{Future, Promise, Return, Throw}

// Currently we simply don't support streaming responses through this
// interface. This can be tackled in a number of ways:
//
//   - a similar continuation-future passing scheme
//   - create a Future[] subclass (eg. ContinuingFuture) that has the
//     logical flatMap implementation.

class ReplyIsStreamingException extends Exception
class CancelledRequestException extends Exception
class InvalidMessageTypeException extends Exception

class Client[Req <: AnyRef, Rep <: AnyRef](broker: Broker)
  extends Stub[Req, Rep]
{
  def call(request: Req): Future[Rep] = {
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
          case Reply.Done(reply: Rep) =>
            promise() = Return(reply)
          case Reply.Done(_) =>
            promise() = Throw(new InvalidMessageTypeException)
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

