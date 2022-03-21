package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncMeter
import com.twitter.finagle.Failure
import com.twitter.finagle.Service
import com.twitter.finagle.SimpleFilter
import com.twitter.util.Future
import com.twitter.util.Throw

import java.util.concurrent.RejectedExecutionException

/**
 * A [[com.twitter.finagle.Filter]] that rate limits requests to a fixed rate over time by
 * using the [[com.twitter.concurrent.AsyncMeter]] implementation. It can be used for slowing
 * down access to throttled resources. Requests that cannot be enqueued to await a permit are failed
 * immediately with a [[com.twitter.finagle.Failure]] that signals that the work was never done,
 * so it's safe to reenqueue.
 *
 * NOTE: If you're just trying not to be overwhelmed, you almost certainly want to use
 * [[com.twitter.finagle.filter.RequestSemaphoreFilter]] instead, because RequestMeterFilter
 * doesn't work well with "real" resources that are sometimes faster or slower (like a service
 * that you're depending on that sometimes slows when it takes bursty traffic). This is better for
 * resources that are artificially bounded, like a rate-limited API.
 */
class RequestMeterFilter[Req, Rep](meter: AsyncMeter) extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    meter.await(1).transform {
      case Throw(noPermit) =>
        noPermit match {
          case _: RejectedExecutionException =>
            Future.exception(Failure.rejected(noPermit))
          case e => Future.exception(e)
        }
      case _ => service(request)
    }
  }
}
