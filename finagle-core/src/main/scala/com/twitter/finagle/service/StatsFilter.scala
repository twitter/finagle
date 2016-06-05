package com.twitter.finagle.service

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.stats.{ExceptionStatsHandler, MultiCategorizingExceptionStatsHandler, StatsReceiver}
import com.twitter.util.{Future, Stopwatch, Throw, Try}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

object StatsFilter {
  val role = Stack.Role("RequestStats")

  /**
   * Configures a [[StatsFilter.module]] to track latency using the
   * given [[TimeUnit]].
   */
  case class Param(unit: TimeUnit) {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }

  object Param {
    implicit val param = Stack.Param(Param(TimeUnit.MILLISECONDS))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.StatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module4[
      param.Stats,
      param.ExceptionStatsHandler,
      param.ResponseClassifier,
      Param,
      ServiceFactory[Req, Rep]
    ] {
      val role = StatsFilter.role
      val description = "Report request statistics"
      def make(
        _stats: param.Stats,
        _exceptions: param.ExceptionStatsHandler,
        _classifier: param.ResponseClassifier,
        _param: Param,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val param.Stats(statsReceiver) = _stats
        val param.ExceptionStatsHandler(handler) = _exceptions
        val classifier = _classifier.responseClassifier
        if (statsReceiver.isNull)
          next
        else
          new StatsFilter(statsReceiver, classifier, handler, _param.unit).andThen(next)
      }
    }

  /** Basic categorizer with all exceptions under 'failures'. */
  val DefaultExceptions = new MultiCategorizingExceptionStatsHandler(
    mkFlags = Failure.flagsOf,
    mkSource = SourcedException.unapply)

  private val SyntheticException =
    new ResponseClassificationSyntheticException()

  def typeAgnostic(
    statsReceiver: StatsReceiver,
    exceptionStatsHandler: ExceptionStatsHandler
  ): TypeAgnostic = new TypeAgnostic {
    override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
      new StatsFilter[Req, Rep](statsReceiver, exceptionStatsHandler)
  }
}

/**
 * A `StatsFilter` reports request statistics including number of requests,
 * number successful and request latency to the given [[StatsReceiver]].
 *
 * @param responseClassifier used to determine when a response
 * is successful or not.
 *
 * @param timeUnit this controls what granularity is used for
 * measuring latency.  The default is milliseconds,
 * but other values are valid. The choice of this changes the name of the stat
 * attached to the given [[StatsReceiver]]. For the common units,
 * it will be "request_latency_ms".
 *
 * @note The innocent bystander may find the semantics with respect
 * to backup requests a bit puzzling; they are entangled in legacy.
 * "requests" counts the total number of requests: subtracting
 * "success" from this produces the failure count. However, this
 * doesn't allow for "shadow" requests to be accounted for in
 * "requests". This is why we don't modify metrics for backup
 * request failures.
 */
class StatsFilter[Req, Rep](
    statsReceiver: StatsReceiver,
    responseClassifier: ResponseClassifier,
    exceptionStatsHandler: ExceptionStatsHandler,
    timeUnit: TimeUnit)
  extends SimpleFilter[Req, Rep]
{
  import StatsFilter.SyntheticException

  def this(
    statsReceiver: StatsReceiver,
    exceptionStatsHandler: ExceptionStatsHandler,
    timeUnit: TimeUnit
  ) = this(statsReceiver, ResponseClassifier.Default, exceptionStatsHandler, timeUnit)

  def this(statsReceiver: StatsReceiver, exceptionStatsHandler: ExceptionStatsHandler) =
    this(statsReceiver, exceptionStatsHandler, TimeUnit.MILLISECONDS)

  def this(statsReceiver: StatsReceiver) = this(statsReceiver, StatsFilter.DefaultExceptions)

  private[this] def latencyStatSuffix: String = {
    timeUnit match {
      case TimeUnit.NANOSECONDS => "ns"
      case TimeUnit.MICROSECONDS => "us"
      case TimeUnit.MILLISECONDS => "ms"
      case TimeUnit.SECONDS => "secs"
      case _ => timeUnit.toString.toLowerCase
    }
  }

  private[this] val outstandingRequestCount = new LongAdder()
  private[this] val dispatchCount = statsReceiver.counter("requests")
  private[this] val successCount = statsReceiver.counter("success")
  private[this] val latencyStat = statsReceiver.stat(s"request_latency_$latencyStatSuffix")
  private[this] val outstandingRequestCountGauge =
    statsReceiver.addGauge("pending") { outstandingRequestCount.sum() }

  private[this] def isBlackholeResponse(rep: Try[Rep]): Boolean = rep match {
    case Throw(BackupRequestLost) | Throw(WriteException(BackupRequestLost)) =>
      // We blackhole this request. It doesn't count for anything.
      // After the Failure() patch, this should no longer need to
      // be a special case.
      //
      // In theory, we should probably unwind the whole cause
      // chain to look for a BackupRequestLost, but in practice it
      // is wrapped only once.
      true
    case _ =>
      false
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val elapsed = Stopwatch.start()

    outstandingRequestCount.increment()
    service(request).respond { response =>
      outstandingRequestCount.decrement()
      if (!isBlackholeResponse(response)) {
        dispatchCount.incr()
        responseClassifier.applyOrElse(
          ReqRep(request, response),
          ResponseClassifier.Default
        ) match {
          case ResponseClass.Failed(_) =>
            latencyStat.add(elapsed().inUnit(timeUnit))
            response match {
              case Throw(e) =>
                exceptionStatsHandler.record(statsReceiver, e)
              case _ =>
                exceptionStatsHandler.record(statsReceiver, SyntheticException)
            }
          case ResponseClass.Successful(_) =>
            successCount.incr()
            latencyStat.add(elapsed().inUnit(timeUnit))
        }
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
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else new StatsServiceFactory(next, statsReceiver)
      }
    }
}

class StatsServiceFactory[Req, Rep](
    factory: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver)
  extends ServiceFactoryProxy[Req, Rep](factory) {
  private[this] val availableGauge = statsReceiver.addGauge("available") {
    if (isAvailable) 1F else 0F
  }
}
