package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Function => _, _}

object RetryingService {
  /**
   * Returns a filter that will retry numTries times, but only if encountering a
   * WriteException.
   */
  def tries[Req, Rep](numTries: Int, stats: StatsReceiver): SimpleFilter[Req, Rep] = {
    val fakeTimer = new Timer {
      def schedule(when: Time)(f: => Unit): TimerTask = throw new Exception("illegal use!")
      def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = throw new Exception("illegal use!")
      def stop() { throw new Exception("illegal use!") }
    }

    val policy = RetryPolicy.tries(numTries)
    new RetryingFilter[Req, Rep](policy, fakeTimer, stats)
  }
}

object RetryingFilter {
  def apply[Req, Rep](
    backoffs: Stream[Duration],
    statsReceiver: StatsReceiver = NullStatsReceiver
  )(shouldRetry: PartialFunction[Try[Nothing], Boolean])(implicit timer: Timer) =
    new RetryingFilter[Req, Rep](RetryPolicy.backoff(backoffs)(shouldRetry), timer, statsReceiver)
}

/**
 * A [[com.twitter.finagle.Filter]] that coordinatess retries of subsequent
 * [[com.twitter.finagle.Service Services]]. Requests can be classified as retryable via
 * the argument [[com.twitter.finagle.service.RetryPolicy]].
 */
class RetryingFilter[Req, Rep](
  retryPolicy: RetryPolicy[Try[Nothing]],
  timer: Timer,
  statsReceiver: StatsReceiver = NullStatsReceiver
) extends SimpleFilter[Req, Rep] {
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

  private[this] def dispatch(
    req: Req,
    service: Service[Req, Rep],
    policy: RetryPolicy[Try[Nothing]],
    count: Int = 0
  ): Future[Rep] = {
    service(req) onSuccess { _ =>
      retriesStat.add(count)
    } rescue { case e =>
      policy(Throw(e)) match {
        case Some((howlong, nextPolicy)) =>
          schedule(howlong) {
            Trace.record("finagle.retry")
            dispatch(req, service, nextPolicy, count + 1)
          }
        case None =>
          retriesStat.add(count)
          Future.exception(e)
      }
    }
  }

  def apply(request: Req, service: Service[Req, Rep]) = dispatch(request, service, retryPolicy)
}
