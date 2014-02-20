package com.twitter.finagle.service

import collection._
import com.twitter.util.{Duration, Time, Future}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{RefusedByRateLimiter, Service, SimpleFilter}

/**
 * Strategy responsible for tracking requests and computing rate per client.
 */
class LocalRateLimitingStrategy[Req](
  categorizer: Req => String,
  windowSize: Duration,
  rate: Int
) extends (Req => Future[Boolean]) {

  private[this] val rates = mutable.HashMap.empty[String, (Int,Time)]

  def apply(req: Req) = synchronized {
    val now = Time.now
    val id = categorizer(req)
    val (remainingRequests, timestamp) = rates.getOrElse(id, (rate, now))

    val accept = if (timestamp.until(now) > windowSize) {
      rates(id) = (rate, now)
      true
    } else {
      if (remainingRequests > 0) {
        rates(id) = (remainingRequests - 1, timestamp)
        true
      } else
        false
    }

    Future.value(accept)
  }
}


/**
 * A [[com.twitter.finagle.Filter]] that accepts or refuses requests based on a
 * rate limiting strategy.
 */
class RateLimitingFilter[Req, Rep](
  strategy: Req => Future[Boolean],
  statsReceiver: StatsReceiver = NullStatsReceiver
) extends SimpleFilter[Req, Rep] {

  private[this] val refused = statsReceiver.counter("refused")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    strategy(request) flatMap { isAuthorized =>
      if (isAuthorized)
        service(request)
      else {
        refused.incr()
        Future.exception(new RefusedByRateLimiter)
      }
    }
}
