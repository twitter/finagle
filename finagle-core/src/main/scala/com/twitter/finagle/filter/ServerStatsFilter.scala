package com.twitter.finagle.filter

import com.twitter.finagle.Deadline
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{param, Service, ServiceFactory, SimpleFilter, Stack, Stackable}
import com.twitter.util.{Future, Stopwatch, Time, Duration}
import java.util.concurrent.TimeUnit

private[finagle] object ServerStatsFilter {
  val role = Stack.Role("ServerStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.ServerStatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = ServerStatsFilter.role
      val description = "Record elapsed execution time, transit latency, deadline budget, of underlying service"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = _stats
        new ServerStatsFilter(statsReceiver).andThen(next)
      }
    }

  /** Used as a sentinel with reference equality to indicate the absence of a deadline */
  private val NoDeadline = Deadline(Time.Undefined, Time.Undefined)
  private val NoDeadlineFn = () => NoDeadline
}

/**
 * A [[com.twitter.finagle.Filter]] that records the elapsed execution
 * times of the underlying [[com.twitter.finagle.Service]], transit
 * time, and budget time.
 *
 * @note the stat does not include the time that it takes to satisfy
 *       the returned `Future`, only how long it takes for the `Service`
 *       to return the `Future`.
 */
private[finagle] class ServerStatsFilter[Req, Rep](statsReceiver: StatsReceiver, nowNanos: () => Long)
  extends SimpleFilter[Req, Rep]
{
  import ServerStatsFilter._

  def this(statsReceiver: StatsReceiver) = this(statsReceiver, Stopwatch.systemNanos)

  private[this] val handletime = statsReceiver.stat("handletime_us")
  private[this] val transitTimeStat = statsReceiver.stat("transit_latency_ms")
  private[this] val budgetTimeStat = statsReceiver.stat("deadline_budget_ms")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val startAt = nowNanos()

    val dl = Contexts.broadcast.getOrElse(Deadline, NoDeadlineFn)
    if (dl ne NoDeadline) {
      val now = Time.now
      transitTimeStat.add(((now-dl.timestamp) max Duration.Zero).inUnit(TimeUnit.MILLISECONDS))
      budgetTimeStat.add(((dl.deadline-now) max Duration.Zero).inUnit(TimeUnit.MILLISECONDS))
    }

    try service(request)
    finally {
      val elapsedNs = nowNanos() - startAt
      handletime.add(TimeUnit.MICROSECONDS.convert(elapsedNs, TimeUnit.NANOSECONDS))
    }
  }
}
