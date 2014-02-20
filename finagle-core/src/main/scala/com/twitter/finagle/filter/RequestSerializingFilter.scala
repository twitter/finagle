package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncMutex
import com.twitter.util.Future
import com.twitter.finagle.{Service, SimpleFilter}

/**
 * A [[com.twitter.finagle.Filter]] that enforces request serialization,
 * meaning that there is guaranteed to be only one outstanding request being
 * serviced at any given time. Request concurrency is restricted by a
 * [[com.twitter.concurrent.AsyncMutex]].
 *
 * This filter is effectively equivalent to a
 * [[com.twitter.finagle.filter.RequestSemaphoreFilter]] constructed with a
 * [[com.twitter.concurrent.AsyncSemaphore]] no `initialPermits` or
 * `maxWaiters`.
 */
class RequestSerializingFilter[Req, Rep] extends SimpleFilter[Req, Rep]  {
  private[this] val mu = new AsyncMutex
  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = mu.acquire() flatMap { permit =>
    service(req) ensure { permit.release() }
  }
}
