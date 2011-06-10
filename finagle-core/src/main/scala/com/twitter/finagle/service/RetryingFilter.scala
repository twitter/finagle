package com.twitter.finagle.service

import com.twitter.util._
import com.twitter.finagle.WriteException
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{SimpleFilter, Service}

object RetryingService {
  /**
   * Returns a filter that will retry numTries times, but only if encountering a 
   * WriteException.
   */
  def tries[Req, Rep](numTries: Int, stats: StatsReceiver): SimpleFilter[Req, Rep] =
    RetryingFilter[Req, Rep](new NumTriesRetryStrategy[Rep](numTries), stats) {
      case Throw(ex: WriteException) => true
    }
}

/**
  * RetryStrategies implement the "how" of retrying (immediate, exponential
  * backoff, etc.) Given the current strategy, nextStrategy should return the
  * next one. It's a Future so that you can do things like waiting for backoff
  * (exponential or non).
  */
trait RetryStrategy[Rep] {
  def nextStrategy: Future[Option[RetryStrategy[Rep]]]
}

object RetryingFilter {
  def apply[Req, Rep](
    retryStrategy: RetryStrategy[Rep],
    statsReceiver: StatsReceiver = NullStatsReceiver
    )(shouldRetry: PartialFunction[Try[Rep], Boolean]) =
      new RetryingFilter[Req, Rep](retryStrategy, statsReceiver, shouldRetry)
}

/**
 * RetryingFilter will coördinates retries. The classification of requests as
 * retryable is done by the PartialFunction shouldRetry and the RetryStrategy
 * will provide the retry implementation (backoff, immediate retry, etc.)
 */
class RetryingFilter[Req, Rep](
    retryStrategy: RetryStrategy[Rep],
    statsReceiver: StatsReceiver = NullStatsReceiver,
    shouldRetry: PartialFunction[Try[Rep], Boolean])
  extends SimpleFilter[Req, Rep]
{
  private[this] val retriesStats = statsReceiver.stat("retries")

  private[this] def dispatch(
    request: Req, service: Service[Req, Rep],
    replyPromise: Promise[Rep],
    strategy: RetryStrategy[Rep],
    count: Int = 0
  ) {
    service(request) respond { res =>
      if (shouldRetry.isDefinedAt(res) && shouldRetry(res)) {
        strategy.nextStrategy respond {
          case Return(Some(nextStrategy)) =>
            dispatch(request, service, replyPromise, nextStrategy, count + 1)
          case Return(None) =>
            retriesStats.add(count)
            replyPromise() = res
          case Throw(_) =>
            replyPromise() = Throw(new IllegalStateException("failure in retry strategy"))
        }
      } else {
        retriesStats.add(count)
        replyPromise() = res
      }
    }
  }

  def apply(request: Req, service: Service[Req, Rep]) = {
    val promise = new Promise[Rep]
    dispatch(request, service, promise, retryStrategy)
    promise
  }
}

class NumTriesRetryStrategy[Rep](numTries: Int) extends RetryStrategy[Rep] {
  def nextStrategy: Future[Option[RetryStrategy[Rep]]] = Future.value {
    if (numTries > 0)
      Some(new NumTriesRetryStrategy[Rep](numTries - 1))
    else
      None
  }
}
