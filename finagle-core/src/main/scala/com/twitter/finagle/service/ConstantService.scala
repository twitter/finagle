package com.twitter.finagle.service

/**
 * Services with constant replies.
 */

import com.twitter.util.Future
import com.twitter.finagle.Service

class ConstantService[Req, Rep](reply: Future[Rep])
  extends Service[Req, Rep]
{
  def apply(request: Req): Future[Rep] = reply
}

class FailedService[Req, Rep](failure: Throwable)
  extends ConstantService[Req, Rep](Future.exception(failure))
{
  override def isAvailable = false
}
