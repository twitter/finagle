package com.twitter.finagle.exp

import com.netflix.concurrency.limits.limit.VegasLimit
import com.netflix.concurrency.limits.Limit
import com.twitter.finagle._
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec

private[finagle] object ConcurrencyLimitFilter {
  class ConcurrencyOverload(val flags: Long = FailureFlags.Rejected)
      extends Exception("Concurrency limit exceeded")
      with FailureFlags[ConcurrencyOverload]
      with HasLogLevel {

    def logLevel: Level = Level.DEBUG
    protected def copyWithFlags(flags: Long): ConcurrencyOverload = new ConcurrencyOverload(flags)
  }

  /**
   * Constant values used for limit algorithm
   *
   * Initial Limit: Number of concurrent requests the limiter initializes with.
   * Typically lower than the true limit. As the limit algorithm receives more
   * samples, the limit will be adjusted from this level.
   *
   * Max Limit: Maximum allowable concurrency. Any estimated concurrency will be capped
   * at this value
   *
   * Alpha: When queue_use is small, limit increases by alpha. When queue_use is large,
   * limit decreases by alpha. Typically 2-3
   *
   * Queue size is calculated using the formula,
   * queue_use = limit × (1 − RTTnoLoad/RTTactual)
   *
   * Beta: Typically 4-6
   *
   * Smoothing: factor (0 < x < 1) to limit how aggressively the estimated limit can shrink
   * when queuing has been detected.
   *
   * StartTime: Defaults to 0 for Vegas and Gradient2 limit algorithm. Used in WindowedLimit to
   * calculate next update time
   */
  private val DefaultInitialLimit: Int = 10
  private val DefaultMaxLimit: Int = 50
  private val DefaultAlpha: Int = 3
  private val DefaultBeta: Int = 6
  private val DefaultSmoothing: Double = 0.6
  private val StartTime: Long = 0

  val role: Stack.Role = Stack.Role("ConcurrencyLimitFilter")

  trait Param

  object Param {

    /**
     * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
     * [[com.twitter.finagle.exp.ConcurrencyLimitFilter]] module.
     */
    case class Configured(initialLimit: Int, maxLimit: Int) extends Param

    case object Disabled extends Param

    implicit val param: Stack.Param[ConcurrencyLimitFilter.Param] =
      Stack.Param(Configured(DefaultInitialLimit, DefaultMaxLimit))
  }

  private[finagle] val Disabled: Param = Param.Disabled

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[ConcurrencyLimitFilter.Param, param.Stats, ServiceFactory[Req, Rep]] {

      val description: String = "Enforce dynamic concurrency limit"
      val role: Stack.Role = ConcurrencyLimitFilter.role

      def make(
        _param: Param,
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = _param match {
        case Param.Configured(initialLimit, maxLimit) =>
          val param.Stats(stats) = _stats
          val filter = new ConcurrencyLimitFilter[Req, Rep](
            VegasLimit
              .newBuilder()
              .alpha(DefaultAlpha)
              .beta(DefaultBeta)
              .smoothing(DefaultSmoothing)
              .initialLimit(initialLimit)
              .maxConcurrency(maxLimit)
              .build(),
            Stopwatch.systemNanos,
            stats.scope("concurrency_limit")
          )
          filter.andThen(next)
        case _ =>
          next
      }
    }

  /**
   * A [[com.twitter.finagle.Filter]] that calculates and enforces request concurrency limit.
   * Incoming requests that exceed current limit are failed immediately with a
   * [[com.twitter.finagle.FailureFlags.Rejected]]
   *
   * @param limit Builder for algorithm that calculates estimated limit
   */
  private[finagle] final class ConcurrencyLimitFilter[Req, Rep](
    limit: Limit,
    now: () => Long,
    statsReceiver: StatsReceiver)
      extends SimpleFilter[Req, Rep] {

    private[this] val rejections: Counter = statsReceiver.counter("dropped_requests")
    private[exp] val pending = new AtomicInteger(0)
    private[this] val pendingGauge = statsReceiver.addGauge("pending") { pending.get() }
    private[this] val estimatedLimit = statsReceiver.addGauge("estimated_concurrency_limit") {
      limit.getLimit()
    }

    /**
     * Updates limit with every request
     *
     * @param start   Time (in nanoseconds) when the request reached this filter
     * @param didDrop Whether the request returned successfully or not
     *
     * limit.onSample(...) takes
     * - startTime: 0
     * - rtt: round trip time of request
     * - inflight: number of outstanding requests
     * - didDrop: success of request
     */
    private[this] def updateLimit(start: Long, didDrop: Boolean): Unit = {
      limit.onSample(StartTime, now() - start, pending.getAndDecrement, didDrop)
    }

    @tailrec
    def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
      val currentPending = pending.get

      if (currentPending >= limit.getLimit) {
        rejections.incr()
        Future.exception(new ConcurrencyOverload)
      } else if (pending.compareAndSet(currentPending, currentPending + 1)) {
        val start = now()
        service(req).respond {
          case Return(_) =>
            updateLimit(start, false)
          case Throw(_) =>
            updateLimit(start, true)
        }
      } else {
        apply(req, service)
      }
    }
  }
}
