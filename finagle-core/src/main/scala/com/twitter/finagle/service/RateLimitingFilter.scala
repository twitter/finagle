package com.twitter.finagle.service

import collection._
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Duration, Time, Future}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}

/**
 * Trait defining a store for accepted requests time
 */
trait RateLimitingStore[Req] {
  def categorize(req: Req): String
  def registerApproval(request: Req)
  def getWindow(request: Req, now: Time, windowSize: Duration): List[Time]
}

/**
 * Local implementation of a request store, every request Time are stored in a HashMap
 */
class LocalRateLimitingStore[Req](categorizer: Req => String) extends RateLimitingStore[Req] {
  private[this] val windows = mutable.HashMap.empty[String, List[Time]]

  def categorize(request: Req) = categorizer(request)

  def registerApproval(request: Req) {
    val id = categorize(request)
    windows(id) = Time.now :: windows.getOrElse(id, Nil)
  }

  def getWindow(request: Req, now: Time, windowSize: Duration): List[Time] = {
    val id = categorize(request)
    windows.getOrElse(id, Nil) takeWhile { _.until(now) < windowSize }
  }
}

/**
 * Filter responsible for accepting/refusing request based on the rate
 * The filter store an "next availability time" representing the next time the channel will become
 * available. Thus if we flood the Rate Limiter, the overhead will be minor.
 */
class RateLimitingFilter[Req, Rep](
  windowSize: Duration,
  rate: Long,
  store: RateLimitingStore[Req],
  statsReceiver: StatsReceiver = NullStatsReceiver
)
  extends SimpleFilter[Req, Rep]
{
  class RefusedByRateLimiter extends Exception("Request refused by rate limiter")

  private[this] val availabilityTime = mutable.HashMap.empty[String, Time]
  private[this] val refused = statsReceiver.counter("req_refused_by_rate_limiter")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val now = Time.now
    val id = store.categorize(request)

    val nextAvailability = availabilityTime.getOrElse(id, Time.epoch)
    if (now < nextAvailability)
      refuseRequest()
    else {
      val window = store.getWindow(request, now, windowSize)
      if (rate <= window.size) {
        availabilityTime(id) = window.last + windowSize
        refuseRequest()
      }
      else
        acceptRequest(request, service)
    }
  }

  private[this] def acceptRequest(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    store.registerApproval(request)
    service(request)
  }

  private[this] def refuseRequest(): Future[Rep] = {
    refused.incr()
    Future.exception(new RefusedByRateLimiter)
  }
}
