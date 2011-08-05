package com.twitter.finagle.service

import java.{util => ju}
import scala.collection.JavaConversions._

import com.twitter.util.{
  Future, Promise, Try, Return, Throw,
  Timer, TimerTask, Time, Duration}
import com.twitter.conversions.time._
import com.twitter.finagle.WriteException
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{SimpleFilter, Service}

object RetryingService {
  /**
   * Returns a filter that will retry numTries times, but only if encountering a
   * WriteException.
   */
  def tries[Req, Rep](numTries: Int, stats: StatsReceiver): SimpleFilter[Req, Rep] = {
      implicit val fakeTimer = new Timer {
        def schedule(when: Time)(f: => Unit): TimerTask = throw new Exception("illegal use!")
        def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = throw new Exception("illegal use!")
        def stop() { throw new Exception("illegal use!") }
      }
      RetryingFilter[Req, Rep](Backoff.const(0.second) take (numTries - 1), stats) {
        case Throw(ex: WriteException) => true
      }
    }
}

object RetryingFilter {
  def apply[Req, Rep](
    backoffs: Stream[Duration],
    statsReceiver: StatsReceiver = NullStatsReceiver
    )(shouldRetry: PartialFunction[Try[Rep], Boolean])(implicit timer: Timer) =
      new RetryingFilter[Req, Rep](backoffs, statsReceiver, shouldRetry, timer)
}

/**
 * RetryingFilter will coördinates retries.  The classification of
 * requests as retryable is done by the PartialFunction shouldRetry
 * and the stream of backoffs.  This stream is consulted to get the
 * next backoff (Duration) after it has been determined a request must
 * be retried.
 */
class RetryingFilter[Req, Rep](
    backoffs: Stream[Duration],
    statsReceiver: StatsReceiver = NullStatsReceiver,
    shouldRetry: PartialFunction[Try[Rep], Boolean],
    timer: Timer)
  extends SimpleFilter[Req, Rep]
{
  private[this] val retriesStats = statsReceiver.stat("retries")

  private[this] def dispatch(
    request: Req, service: Service[Req, Rep],
    replyPromise: Promise[Rep],
    backoffs: Stream[Duration],
    count: Int = 0
  ) {
    val res = service(request) respond { res =>
      if (shouldRetry.isDefinedAt(res) && shouldRetry(res)) {
        backoffs match {
          case howlong #:: rest if howlong > 0.seconds =>
            timer.schedule(Time.now + howlong) {
              dispatch(request, service, replyPromise, rest, count + 1)
            }

          case howlong #:: rest =>
            dispatch(request, service, replyPromise, rest, count + 1)

          case _ =>
            retriesStats.add(count)
            replyPromise() = res
        }
      } else {
        retriesStats.add(count)
        replyPromise() = res
      }
    }
    replyPromise.linkTo(res)
  }

  def apply(request: Req, service: Service[Req, Rep]) = {
    val promise = new Promise[Rep]
    dispatch(request, service, promise, backoffs)
    promise
  }
}

/**
 * Implements various backoff strategies.
 */
object Backoff {
  private[this] def durations(next: Duration, f: Duration => Duration): Stream[Duration] =
    next #:: durations(f(next), f)

  def apply(next: Duration)(f: Duration => Duration) = durations(next, f)

  def exponential(start: Duration, multiplier: Int) =
    Backoff(start) { _ * multiplier }

  def linear(start: Duration, offset: Duration) =
    Backoff(start) { _ + offset }

  def const(start: Duration) =
    Backoff(start)(Function.const(start))

  /**
   * Convert a {{Stream[Duration]}} into a Java-friendly representation.
   */
  def toJava(backoffs: Stream[Duration]): ju.concurrent.Callable[ju.Iterator[Duration]] = {
    new ju.concurrent.Callable[ju.Iterator[Duration]] {
      def call() = backoffs.toIterator
    }
  }
}
