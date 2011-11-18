package com.twitter.finagle.service

import java.{util => ju}
import java.util.{concurrent => juc}
import scala.collection.JavaConversions._
import com.twitter.conversions.time._
import com.twitter.finagle.{RetryFailureException, SimpleFilter, Service, WriteException}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util._
import com.twitter.finagle.tracing.Trace

trait RetryPolicy[-A] extends (A => Option[(Duration, RetryPolicy[A])])

object RetryPolicy {
  val WriteExceptionsOnly: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(_: WriteException) => true
  }

  def tries(numTries: Int) = {
    backoff[Try[Nothing]](Backoff.const(0.second) take (numTries - 1))(WriteExceptionsOnly)
  }

  /**
   * A constructor usable from Java (`backoffs` from `Backoff.toJava`).
   */
  def backoffJava[A](
    backoffs: juc.Callable[ju.Iterator[Duration]],
    shouldRetry: PartialFunction[A, Boolean]
  ): RetryPolicy[Try[Nothing]] = {
    backoff[Try[Nothing]](backoffs.call().toStream) {
      case Throw(_: WriteException) => true
    }
  }

  def backoff[A](
    backoffs: Stream[Duration]
  )(shouldRetry: PartialFunction[A, Boolean]): RetryPolicy[A] = {
    new RetryPolicy[A] {
      def apply(e: A) = {
        if (shouldRetry.isDefinedAt(e) && shouldRetry(e)) {
          backoffs match {
            case howlong #:: rest =>
              Some((howlong, backoff(rest)(shouldRetry)))
            case _ =>
              None
          }
        } else {
          None
        }
      }
    }
  }
}

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
  )(shouldRetry: PartialFunction[Try[Rep], Boolean])(implicit timer: Timer) =
    new RetryingFilter[Req, Rep](RetryPolicy.backoff(backoffs)(shouldRetry), timer, statsReceiver)
}

/**
 * RetryingFilter will coordinate retries. The classification of
 * requests as retryable is done by the PartialFunction shouldRetry
 * and the stream of backoffs. This stream is consulted to get the
 * next backoff (Duration) after it has been determined a request must
 * be retried.
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
        val rep = f
        promise.linkTo(rep)
        rep.proxyTo(promise)
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

  /* Alias because `const' is a reserved word in Java */
  def constant(start: Duration) = const(start)

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
