package com.twitter.finagle.filter

import com.twitter.finagle.context.Deadline
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{param, Service, ServiceFactory, SimpleFilter, Stack, Stackable}
import com.twitter.util.{Time, Duration, Future, Stopwatch}
import java.util.concurrent.TimeUnit

private[finagle] object ServerStatsFilter {
  val role = Stack.Role("ServerStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.ServerStatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = ServerStatsFilter.role
      val description =
        "Record elapsed execution time, transit latency, deadline budget, of underlying service"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else new ServerStatsFilter(statsReceiver).andThen(next)
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that records the elapsed execution
 * times of the underlying [[com.twitter.finagle.Service]].
 *
 * @note the stat does not include the time that it takes to satisfy
 *       the returned `Future`, only how long it takes for the `Service`
 *       to return the `Future`.
 */
private[finagle] class ServerStatsFilter[Req, Rep](
  statsReceiver: StatsReceiver,
  nowNanos: () => Long)
    extends SimpleFilter[Req, Rep] {
  def this(statsReceiver: StatsReceiver) = this(statsReceiver, Stopwatch.systemNanos)

  private[this] val handletime = statsReceiver.stat("handletime_us")
  private[this] val transitTimeStat = statsReceiver.stat("transit_latency_ms")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val startAt = nowNanos()

    Deadline.current match {
      case Some(deadline) =>
        val now = Time.now
        transitTimeStat.add((now - deadline.timestamp).max(Duration.Zero).inMillis)
      case None =>
    }

    try service(request)
    finally {
      val elapsedNs = nowNanos() - startAt
      handletime.add(TimeUnit.MICROSECONDS.convert(elapsedNs, TimeUnit.NANOSECONDS))
    }
  }
}
