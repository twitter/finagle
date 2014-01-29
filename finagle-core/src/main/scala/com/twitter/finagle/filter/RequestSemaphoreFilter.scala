package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.{Service, SimpleFilter}

/**
 * A [[com.twitter.finagle.Filter]] that restricts request concurrency according
 * to an argument [[com.twitter.concurrent.AsyncSemaphore]]. The number of
 * concurrently-applied subsequent [[com.twitter.finagle.Service Services]] is
 * bounded by the rate of permits doled out by the semaphore.
 */
class RequestSemaphoreFilter[Req, Rep](sem: AsyncSemaphore) extends SimpleFilter[Req, Rep] {
  def apply(req: Req, service: Service[Req, Rep]) = sem.acquire() flatMap { permit =>
    service(req) ensure { permit.release() }
  }
}
