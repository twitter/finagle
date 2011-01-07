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
  def apply(request: Req): Future[Rep] =
    broker(request) flatMap { reply =>
      if (reply.isInstanceOf[Rep])
        Return(reply.asInstanceOf[Rep])
      else
        Throw(new InvalidMessageTypeException)
    }
}

