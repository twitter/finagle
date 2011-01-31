package com.twitter.finagle.loadbalancer

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.{Time, Duration, Throw, Return}
import com.twitter.conversions.time._

import com.twitter.finagle.Service

/**
 * A simple failure accrual strategy. Given an underlying load
 * balancer & parameters (# of consecutive failures, and "dead time"),
 * services that are considered dead are excluded from further
 * dispatch until they are again revived.
 */
class FailureAccrualStrategy[Req, Rep](
    underlying: LoadBalancerStrategy[Req, Rep],
    numFailures: Int,
    markDeadFor: Duration)
  extends LoadBalancerStrategy[Req, Rep]
{
  // This will only admit one request after having failed (ie. isDead
  // is none), but any more & it's re-marked.
  private[this] class FailureMetadata {
    private[this] var failureCount = 0
    private[this] var failedAt = Time.epoch

    def isDead = synchronized { failedAt.untilNow < markDeadFor }

    def didFail() = synchronized {
      failureCount += 1
      if (failureCount >= numFailures)
        failedAt = Time.now
    }

    def didSucceed() = synchronized {
      failureCount = 0
      failedAt = Time.epoch
    }
  }

  private[this] val meta = ServiceMetadata[FailureMetadata] { new FailureMetadata }
  private[this] val failureCount =
    new ServiceMetadata[AtomicInteger](new AtomicInteger(0))
  private[this] val failedAt = new ServiceMetadata[Time](Time.epoch)

  def dispatch(request: Req, services: Seq[Service[Req, Rep]]) = {
    // Compute the set of live services. If none are alive, we pass
    // the entire service set down.
    val alive    = services filter { service => !meta(service).isDead }
    val filtered = if (alive.isEmpty) services else alive
    val result   = underlying.dispatch(request, filtered)

    result foreach { case (service, resultFuture) =>
      resultFuture respond {
        case Throw(_)  => meta(service).didFail()
        case Return(_) => meta(service).didSucceed()
      }
    }

    result
  }
}
