package com.twitter.finagle.service

/**
 * Services with constant replies.
 */

import com.twitter.util.Future
import com.twitter.finagle.Service

class ConstantService[Req, Rep](reply: Future[Rep]) extends Service[Req, Rep] {
  def apply(request: Req): Future[Rep] = reply
}

class FailedService(failure: Throwable)
  extends ConstantService[Any, Nothing](Future.exception(failure))
{
  override def isAvailable = false
}

object NilService extends FailedService(new Exception("dispatch to invalid service"))
