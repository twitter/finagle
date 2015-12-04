package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncMeter
import com.twitter.finagle.{Failure, Service, SimpleFilter}
import com.twitter.util.{Future, Throw}

/**
  * A [[com.twitter.finagle.Filter]] that rate limits requests to a fixed rate over time by
  * using the [[com.twitter.concurrent.AsyncMeter]] implementation. It can be used for slowing
  * down access to throttled resources. Requests that are unable to acquire a permit are failed
  * immediately with a [[com.twitter.finagle.Failure]] that signals a restartable or
  * idempotent process.
  */
class RequestMeterFilter[Req, Rep](meter: AsyncMeter, permits: Int = 1) extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]) = {
    meter.await(permits).transform {
      case Throw(noPermit) => Future.exception(Failure.rejected(noPermit))
      case _ => service(request)
    }
  }
}
