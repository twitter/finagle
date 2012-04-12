package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.{Service, SimpleFilter}

class RequestSemaphoreFilter[Req, Rep](sem: AsyncSemaphore) extends SimpleFilter[Req, Rep] {
  def apply(req: Req, service: Service[Req, Rep]) = sem.acquire() flatMap { permit =>
    service(req) ensure { permit.release() }
  }
}
