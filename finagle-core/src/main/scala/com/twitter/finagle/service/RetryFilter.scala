package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.{Filter, Service}
import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Function => _, _}

object RetryingService {

  /**
   * Returns a filter that will retry a failed request a total of
   * `numTries - 1` times, but only when the failure encountered
   * is a [[com.twitter.finagle.WriteException WriteException]].
   */
  def tries[Req, Rep](numTries: Int, stats: StatsReceiver): Filter[Req, Rep, Req, Rep] = {
    val policy = RetryPolicy.tries(numTries)
    new RetryExceptionsFilter[Req, Rep](policy, HighResTimer.Default, stats)
  }
}

/**
 * A [[com.twitter.finagle.Filter]] that coordinates retries of subsequent
 * [[com.twitter.finagle.Service Services]]. Successful and exceptional
 * responses can be classified as retryable via the retryPolicy
 * [[com.twitter.finagle.service.RetryPolicy]] argument.
 *
 * @param retryBudget the budget that is [[RetryBudget.tryWithdraw() withdrawn from]]
 * for retries.
 *
 * @note consider using a [[Timer]] with high resolution so that there is
 * less correlation between retries. For example [[HighResTimer.Default]].
 */
class RetryFilter[Req, Rep](
    retryPolicy: RetryPolicy[(Req, Try[Rep])],
    timer: Timer,
    statsReceiver: StatsReceiver,
    retryBudget: RetryBudget)
  extends Filter[Req, Rep, Req, Rep] {

  /**
   * A [[com.twitter.finagle.Filter]] that coordinates retries of subsequent
   * [[com.twitter.finagle.Service Services]]. Successful and exceptional
   * responses can can be classified as retryable via the retryPolicy
   * [[com.twitter.finagle.service.RetryPolicy]] argument.
   *
   * @note consider using a [[Timer]] with high resolution so that there is
   * less correlation between retries. For example [[HighResTimer.Default]].
   */
  def this(
    retryPolicy: RetryPolicy[(Req, Try[Rep])],
    timer: Timer,
    statsReceiver: StatsReceiver
  ) = this(
    retryPolicy,
    timer,
    statsReceiver,
    RetryBudget()
  )

  private[this] val retriesStat = statsReceiver.stat("retries")

  private[this] val budgetExhausted =
    statsReceiver.scope("retries").counter("budget_exhausted")

  @inline
  private[this] def schedule(d: Duration)(f: => Future[Rep]) = {
    if (d > 0.seconds) {
      val promise = new Promise[Rep]
      timer.schedule(Time.now + d) {
        promise.become(f)
      }
      promise
    } else f
  }

  private[this] def dispatch(
    req: Req,
    service: Service[Req, Rep],
    policy: RetryPolicy[(Req, Try[Rep])],
    count: Int = 0
  ): Future[Rep] = {
    val svcRep = service(req)
    svcRep.transform { rep =>
      policy((req, rep)) match {
        case Some((howlong, nextPolicy)) =>
          if (retryBudget.tryWithdraw()) {
            schedule(howlong) {
              Trace.record("finagle.retry")
              dispatch(req, service, nextPolicy, count + 1)
            }
          } else {
            budgetExhausted.incr()
            retriesStat.add(count)
            svcRep
          }
        case None =>
          retriesStat.add(count)
          svcRep
      }
    }
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    retryBudget.deposit()
    dispatch(request, service, retryPolicy)
  }
}

object RetryFilter {

  /**
   * @param backoffs See [[Backoff]] for common backoff patterns.
   *
   * @note consider using a [[Timer]] with high resolution so that there is
   * less correlation between retries. For example [[HighResTimer.Default]].
   */
  def apply[Req, Rep](
    backoffs: Stream[Duration],
    statsReceiver: StatsReceiver = NullStatsReceiver
  )(
    shouldRetry: PartialFunction[(Req, Try[Rep]), Boolean]
  )(
    implicit timer: Timer
  ): RetryFilter[Req, Rep] =
    new RetryFilter[Req, Rep](RetryPolicy.backoff(backoffs)(shouldRetry), timer, statsReceiver)

}

/**
 * A [[com.twitter.finagle.Filter]] that coordinates retries of subsequent
 * [[com.twitter.finagle.Service Services]]. Exceptional responses can can be
 * classified as retryable via the retryPolicy argument
 * [[com.twitter.finagle.service.RetryPolicy]].
 *
 * @param retryBudget the budget that is [[RetryBudget.tryWithdraw() withdrawn from]]
 * for retries.
 *
 * @note consider using a [[Timer]] with high resolution so that there is
 * less correlation between retries. For example [[HighResTimer.Default]].
 *
 * @see [[RetryFilter]] for a version that allows for retries on "successful"
 * responses as well as failures.
 */
final class RetryExceptionsFilter[Req, Rep](
    retryPolicy: RetryPolicy[Try[Nothing]],
    timer: Timer,
    statsReceiver: StatsReceiver,
    retryBudget: RetryBudget)
  extends RetryFilter[Req, Rep](
    RetryPolicy.convertExceptionPolicy(retryPolicy),
    timer,
    statsReceiver,
    retryBudget)
{

  /**
   * A [[com.twitter.finagle.Filter]] that coordinates retries of subsequent
   * [[com.twitter.finagle.Service Services]]. Exceptional responses can can be
   * classified as retryable via the retryPolicy argument
   * [[com.twitter.finagle.service.RetryPolicy]].
   *
   * @note consider using a [[Timer]] with high resolution so that there is
   * less correlation between retries. For example [[HighResTimer.Default]].
   *
   * @see [[RetryFilter]] for a version that allows for retries on "successful"
   * responses as well as failures.
   */
  def this(
    retryPolicy: RetryPolicy[Try[Nothing]],
    timer: Timer,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ) = this(
    retryPolicy,
    timer,
    statsReceiver,
    RetryBudget()
  )
}

object RetryExceptionsFilter {

  /**
   * @param backoffs See [[Backoff]] for common backoff patterns.
   *
   * @note consider using a [[Timer]] with high resolution so that there is
   * less correlation between retries. For example [[HighResTimer.Default]].
   */
  def apply[Req, Rep](
    backoffs: Stream[Duration],
    statsReceiver: StatsReceiver = NullStatsReceiver
  )(
    shouldRetry: PartialFunction[Try[Nothing], Boolean]
  )(
    implicit timer: Timer
  ): RetryExceptionsFilter[Req, Rep] =
    new RetryExceptionsFilter[Req, Rep](
      RetryPolicy.backoff(backoffs)(shouldRetry),
      timer,
      statsReceiver)

  def typeAgnostic(
    retryPolicy: RetryPolicy[Try[Nothing]],
    timer: Timer,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): TypeAgnostic = new TypeAgnostic {
    override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
      new RetryExceptionsFilter[Req, Rep](retryPolicy, timer, statsReceiver)
  }
}
