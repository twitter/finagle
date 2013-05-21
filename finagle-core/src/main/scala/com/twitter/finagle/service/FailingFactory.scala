package com.twitter.finagle.service

import com.twitter.util.{Future, Time}
import com.twitter.finagle.{ServiceFactory, ClientConnection}

class FailingFactory[Req, Rep](error: Throwable)
  extends ServiceFactory[Req, Rep]
{
  def apply(conn: ClientConnection) = Future.exception(error)
  def close(deadline: Time) = Future.Done
  override def isAvailable = true
  override val toString = "failing_factory_%s".format(error)
}
