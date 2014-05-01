package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Throwables
import com.twitter.util.{Future, Stopwatch, Throw, Return}
import java.util.concurrent.atomic.AtomicInteger

private[finagle] object StatsFilter {
  object RequestStats extends Stack.Role

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.StatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](RequestStats) {
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = params[param.Stats]
        if (statsReceiver.isNull) next
        else new StatsFilter(statsReceiver) andThen next
      }
    }
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
class StatsFilter[Req, Rep](statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep]
{
  private[this] val outstandingRequestCount = new AtomicInteger(0)
  private[this] val dispatchCount = statsReceiver.counter("requests")
  private[this] val successCount = statsReceiver.counter("success")
  private[this] val failureReceiver = statsReceiver.scope("failures")
  private[this] val sourcedFailuresReceiver = statsReceiver.scope("sourcedfailures")
  private[this] val latencyStat = statsReceiver.stat("request_latency_ms")
  private[this] val loadGauge = statsReceiver.addGauge("load") { outstandingRequestCount.get }
  private[this] val outstandingRequestCountGauge =
    statsReceiver.addGauge("pending") { outstandingRequestCount.get }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val elapsed = Stopwatch.start()

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
          failureReceiver.counter(Throwables.mkString(e): _*).incr()
          e match {
            case sourced: SourcedException if sourced.serviceName != "unspecified" =>
              sourcedFailuresReceiver
                .counter(sourced.serviceName +: Throwables.mkString(sourced): _*)
                .incr()
            case _ =>
          }
        case Return(_) =>
          dispatchCount.incr()
          successCount.incr()
          latencyStat.add(elapsed().inMilliseconds)
      }
    }
  }
}

private[finagle] object StatsServiceFactory {
  object FactoryStats extends Stack.Role

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.StatsServiceFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](FactoryStats) {
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = params[param.Stats]
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
