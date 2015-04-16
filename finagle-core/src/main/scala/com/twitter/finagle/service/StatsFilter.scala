package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.{
  CategorizingExceptionStatsHandler, ExceptionStatsHandler, StatsReceiver}
import com.twitter.util.{Future, Stopwatch, Throw, Return, Time, Duration}
import java.util.concurrent.atomic.AtomicInteger

object StatsFilter {
  val role = Stack.Role("RequestStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.StatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Stats, param.ExceptionStatsHandler, ServiceFactory[Req, Rep]] {
      val role = StatsFilter.role
      val description = "Report request statistics"
      def make(_stats: param.Stats, _exceptions: param.ExceptionStatsHandler, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = _stats
        val param.ExceptionStatsHandler(handler) = _exceptions
        if (statsReceiver.isNull) next
        else new StatsFilter(statsReceiver, handler) andThen next
      }
    }

  /** Basic categorizer with all exceptions under 'failures'. */
  val DefaultExceptions = new CategorizingExceptionStatsHandler(
    sourceFunction = SourcedException.unapply(_))
}

/**
 * StatsFilter reports request statistics to the given receiver.
 *
 * @note The innocent bystander may find the semantics with respect
 * to backup requests a bit puzzling; they are entangled in legacy.
 * "requests" counts the total number of requests: subtracting
 * "success" from this produces the failure count. However, this
 * doesn't allow for "shadow" requests to be accounted for in
 * "requests". This is why we don't increment "requests" on backup
 * request failures.
 */
class StatsFilter[Req, Rep](
    statsReceiver: StatsReceiver,
    exceptionStatsHandler: ExceptionStatsHandler)
  extends SimpleFilter[Req, Rep]
{
  def this(statsReceiver: StatsReceiver) = this(statsReceiver, StatsFilter.DefaultExceptions)

  private[this] val outstandingRequestCount = new AtomicInteger(0)
  private[this] val dispatchCount = statsReceiver.counter("requests")
  private[this] val successCount = statsReceiver.counter("success")
  private[this] val latencyStat = statsReceiver.stat("request_latency_ms")
  private[this] val loadGauge = statsReceiver.addGauge("load") { outstandingRequestCount.get }
  private[this] val outstandingRequestCountGauge =
    statsReceiver.addGauge("pending") { outstandingRequestCount.get }
  private[this] val transitTimeStat = statsReceiver.stat("transit_latency_ms")
  private[this] val budgetTimeStat = statsReceiver.stat("deadline_budget_ms")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val elapsed = Stopwatch.start()
    
    Contexts.broadcast.get(Deadline) match {
      case None =>
      case Some(Deadline(timestamp, deadline)) =>
        val now = Time.now
        transitTimeStat.add(((now-timestamp) max Duration.Zero).inMilliseconds)
        budgetTimeStat.add(((deadline-now) max Duration.Zero).inMilliseconds)
    }

    outstandingRequestCount.incrementAndGet()
    service(request) respond { response =>
      outstandingRequestCount.decrementAndGet()
      response match {
        case Throw(BackupRequestLost) | Throw(WriteException(BackupRequestLost)) =>
          // We blackhole this request. It doesn't count for anything.
          // After the Failure() patch, this should no longer need to
          // be a special case.
          //
          // In theory, we should probably unwind the whole cause
          // chain to look for a BackupRequestLost, but in practice it
          // is wrapped only once.
        case Throw(e) =>
          dispatchCount.incr()
          latencyStat.add(elapsed().inMilliseconds)
          exceptionStatsHandler.record(statsReceiver, e)
        case Return(_) =>
          dispatchCount.incr()
          successCount.incr()
          latencyStat.add(elapsed().inMilliseconds)
      }
    }
  }
}

private[finagle] object StatsServiceFactory {
  val role = Stack.Role("FactoryStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.StatsServiceFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = StatsServiceFactory.role
      val description = "Report connection statistics"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else new StatsServiceFactory(next, statsReceiver)
      }
    }
}

class StatsServiceFactory[Req, Rep](
  factory: ServiceFactory[Req, Rep],
  statsReceiver: StatsReceiver
) extends ServiceFactoryProxy[Req, Rep](factory) {
  private[this] val availableGauge = statsReceiver.addGauge("available") {
    if (isAvailable) 1F else 0F
  }
}
