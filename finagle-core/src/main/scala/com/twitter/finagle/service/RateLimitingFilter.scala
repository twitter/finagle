package com.twitter.finagle.service

import collection._
import com.twitter.util.{Duration, Time, Future}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{RefusedByRateLimiter, Service, SimpleFilter}

/**
 * Local implementation of a request store, every request Time are stored in a HashMap
 */
class LocalRateLimitingStrategy[Req](
  categorizer: Req => String,
  windowSize: Duration,
  rate: Int
) extends (Req => Future[Boolean]) {

  private[this] val windows = mutable.HashMap.empty[String, List[Time]]
  private[this] val availabilityTime = mutable.HashMap.empty[String, Time]

  def apply(req: Req) = synchronized {
    val now = Time.now
    val id = categorizer(req)

    val nextAvailability = availabilityTime.getOrElse(id, Time.epoch)
    val accept = if (now < nextAvailability)
      false
    else {
      val window = windows.getOrElse(id, Nil) takeWhile { _.until(now) < windowSize }
      if (rate <= window.size) {
        availabilityTime(id) = window.last + windowSize
        false
      } else {
        windows(id) = Time.now :: windows.getOrElse(id, Nil)
        true
      }
    }

    Future.value(accept)
  }
}

/**
 * Filter responsible for accepting/refusing request based on the rate limiting strategy.
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
