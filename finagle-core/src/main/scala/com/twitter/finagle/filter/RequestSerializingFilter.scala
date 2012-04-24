package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncMutex
import com.twitter.util.Future
import com.twitter.finagle.{Service, SimpleFilter}

class RequestSerializingFilter[Req, Rep] extends SimpleFilter[Req, Rep]  {
  private[this] val mu = new AsyncMutex
  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = mu.acquire() flatMap { permit =>
    service(req) ensure { permit.release() }
  }
}
