package com.twitter.finagle.filter

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{param, Service, ServiceFactory, SimpleFilter, Stack, Stackable}
import com.twitter.util.Future
import java.util.concurrent.TimeUnit

private[finagle] object HandletimeFilter {
  val role = Stack.Role("HandleTime")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.HandletimeFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = HandletimeFilter.role
      val description = "Record elapsed execution time of underlying service"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = _stats
        new HandletimeFilter(statsReceiver).andThen(next)
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that records the elapsed execution times of
 * the underlying [[com.twitter.finagle.Service]]. Durations are recorded in
 * microseconds and emitted as a stat labeled "handletime_us" to the argument
 * [[com.twitter.finagle.stats.StatsReceiver]].
 *
 * @note the stat does not include the time that it takes to satisfy
 *       the returned `Future`, only how long it takes for the `Service`
 *       to return the `Future`.
 */
class HandletimeFilter[Req, Rep](statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep]
{
  private[this] val stat = statsReceiver.stat("handletime_us")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val startAt = System.nanoTime()
    try
      service(request)
    finally {
      val elapsedNs = System.nanoTime() - startAt
      stat.add(TimeUnit.MICROSECONDS.convert(elapsedNs, TimeUnit.NANOSECONDS))
    }
  }
}
