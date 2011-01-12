package com.twitter.finagle.service

import com.twitter.finagle.channel._
import com.twitter.util.{Future, Try}
import com.twitter.finagle.util.Conversions._

class ReplyIsStreamingException   extends Exception
class CancelledRequestException   extends Exception
class InvalidMessageTypeException extends Exception

class Client[-Req, +Rep](broker: Service[Any, Any])
  extends Service[Req, Rep]
{
  def apply(request: Req): Future[Rep] =
    broker(request) flatMap { reply => Try(reply.asInstanceOf[Rep]) }
}

