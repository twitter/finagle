package com.twitter.finagle.service

import com.twitter.util.Future
import com.twitter.finagle.{Service, Status}
import scala.util.control.NoStackTrace

/**
 * A [[com.twitter.finagle.Service]] that returns a constant result.
 */
class ConstantService[Req, Rep](reply: Future[Rep]) extends Service[Req, Rep] {
  def apply(request: Req): Future[Rep] = reply
}

/**
 * A [[com.twitter.finagle.Service]] that fails with a constant Throwable.
 */
class FailedService(failure: Throwable)
  extends ConstantService[Any, Nothing](Future.exception(failure))
{
  override def status: Status = Status.Closed
}

/**
 * A static [[FailedService]] object.
 */
object NilService
  extends FailedService(
    new Exception("dispatch to invalid service") with NoStackTrace)
