package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.util.{Function => _, _}

object RetryingService {

  private[this] val fakeTimer = new Timer {
    def schedule(when: Time)(f: => Unit): TimerTask = throw new Exception("illegal use!")
    def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = throw new Exception("illegal use!")
    def stop() { throw new Exception("illegal use!") }
  }

  /**
   * Returns a filter that will try a total of numTries times, but only if
   * encountering a WriteException.
   */
  def tries[Req, Rep](numTries: Int, stats: StatsReceiver): Filter[Req, Rep, Req, Rep] = {
    val policy = RetryPolicy.tries(numTries)
    new RetryExceptionsFilter[Req, Rep](policy, fakeTimer, stats)
  }
}

/**
 * A [[com.twitter.finagle.Filter]] that coordinates retries of subsequent
 * [[com.twitter.finagle.Service Services]]. Successful and exceptional
 * responses can can be classified as retryable via the retryPolicy
 * [[com.twitter.finagle.service.RetryPolicy]] argument.
 */
class RetryFilter[Req, Rep](
    retryPolicy: RetryPolicy[(Req, Try[Rep])],
    timer: Timer,
    statsReceiver: StatsReceiver)
  extends Filter[Req, Rep, Req, Rep] {

  private[this] val retriesStat = statsReceiver.stat("retries")

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

  protected[this] def dispatch(
    req: Req,
    service: Service[Req, Rep],
    policy: RetryPolicy[(Req, Try[Rep])],
    count: Int = 0
  ): Future[Rep] = service(req).transform { rep =>
    policy((req, rep)) match {
      case Some((howlong, nextPolicy)) =>
        schedule(howlong) {
          Trace.record("finagle.retry")
          dispatch(req, service, nextPolicy, count + 1)
        }
      case None =>
        retriesStat.add(count)
        Future.const(rep)
    }
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    dispatch(request, service, retryPolicy)
}

object RetryFilter {
  def apply[Req, Rep](
    backoffs: Stream[Duration],
    statsReceiver: StatsReceiver = NullStatsReceiver
  )(shouldRetry: PartialFunction[(Req, Try[Rep]), Boolean])(implicit timer: Timer) =
    new RetryFilter[Req, Rep](RetryPolicy.backoff(backoffs)(shouldRetry), timer, statsReceiver)
}

/**
 * A [[com.twitter.finagle.Filter]] that coordinates retries of subsequent
 * [[com.twitter.finagle.Service Services]]. Exceptional responses can can be
 * classified as retryable via the retryPolicy argument
 * [[com.twitter.finagle.service.RetryPolicy]].
 * @see [[RetryFilter]] for a version that allows for retries on "successful"
 * responses as well as failures.
 */
final class RetryExceptionsFilter[Req, Rep](
    retryPolicy: RetryPolicy[Try[Nothing]],
    timer: Timer,
    statsReceiver: StatsReceiver = NullStatsReceiver)
  extends RetryFilter[Req, Rep](
    RetryPolicy.convertExceptionPolicy(retryPolicy),
    timer,
    statsReceiver)

object RetryExceptionsFilter {
  def apply[Req, Rep](
    backoffs: Stream[Duration],
    statsReceiver: StatsReceiver = NullStatsReceiver
  )(shouldRetry: PartialFunction[Try[Nothing], Boolean])(implicit timer: Timer) =
    new RetryExceptionsFilter[Req, Rep](RetryPolicy.backoff(backoffs)(shouldRetry), timer, statsReceiver)

  def typeAgnostic(
    retryPolicy: RetryPolicy[Try[Nothing]],
    timer: Timer,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): TypeAgnostic = new TypeAgnostic {
    override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
      new RetryExceptionsFilter[Req, Rep](retryPolicy, timer, statsReceiver)
  }
}
