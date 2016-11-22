package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Duration, Future, Time}

object DeadlineStatsFilter {

  val role = new Stack.Role("DeadlineStatsFilter")

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.DeadlineStatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = DeadlineStatsFilter.role
      val description = "Records deadline stats for requests"

      def make(
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ) = {
        val param.Stats(statsReceiver) = _stats
        val scopedStatsReceiver = statsReceiver.scope("admission_control", "deadline")
        new DeadlineStatsFilter(scopedStatsReceiver).andThen(next)
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that records the number of requests
 * with exceeded deadlines, the remaining deadline budget, and the
 * transit latency of requests.
 *
 * @param statsReceiver for stats reporting, typically scoped to
 * ".../admission_control/deadline/"
 *
 */
private[finagle] class DeadlineStatsFilter[Req, Rep](statsReceiver: StatsReceiver)
    extends SimpleFilter[Req, Rep] {

  private[this] val exceededStat = statsReceiver.counter("exceeded")
  private[this] val transitTimeStat = statsReceiver.stat("transit_latency_ms")
  private[this] val expiredTimeStat = statsReceiver.stat("expired_ms")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    Deadline.current match {
      case Some(deadline) =>
        val now = Time.now
        if (deadline.deadline < now) {
          val exceeded = now - deadline.deadline
          expiredTimeStat.add(exceeded.inMilliseconds)
        }
        transitTimeStat.add((now - deadline.timestamp).max(Duration.Zero).inMilliseconds)

        if (deadline.expired)
          exceededStat.incr()

      case None =>
    }
    service(request)
  }
}
