package com.twitter.finagle.service

import com.twitter.util._
import com.twitter.finagle.WriteException
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{SimpleFilter, Service}

object RetryingService {
  def tries[Req, Rep](numTries: Int) =
    new RetryingFilter[Req, Rep](new NumTriesRetryStrategy(numTries))
}

trait RetryStrategy {
  def nextStrategy: Future[RetryStrategy]
}

/**
 * The RetryingFilter attempts to replay a request in certain failure
 * conditions.  Currently we define *any* WriteError as a retriable
 * error, but nothing else. We cannot make broader assumptions without
 * application knowledge (eg. the request may be side effecting),
 * except to say that the message itself was not delivered. Any other
 * types of retries must be done in the service stack.
 */
class RetryingFilter[Req, Rep](
    retryStrategy: RetryStrategy,
    statsReceiver: StatsReceiver = NullStatsReceiver)
  extends SimpleFilter[Req, Rep]
{
  private[this] val retriesStats = statsReceiver.stat("retries")

  private[this] def dispatch(
    request: Req, service: Service[Req, Rep],
    replyPromise: Promise[Rep],
    strategy: RetryStrategy, count: Int = 0
  ) {
    service(request) respond {
      // Only write exceptions are retriable.
      case t@Throw(cause) if cause.isInstanceOf[WriteException] =>
        // Time to retry.
        strategy.nextStrategy respond {
          case Return(nextStrategy) =>
            dispatch(request, service, replyPromise, nextStrategy, count + 1)
          case Throw(_) =>
            replyPromise.updateIfEmpty(t)
        }

      case rv =>
        if (count > 0) retriesStats.add(count)
        replyPromise.updateIfEmpty(rv)
    }
  }

  def apply(request: Req, service: Service[Req, Rep]) = {
    val promise = new Promise[Rep]
    dispatch(request, service, promise, retryStrategy)
    promise
  }
}

class NumTriesRetryStrategy(numTries: Int) extends RetryStrategy {
  def nextStrategy = {
    if (numTries > 0)
      Future.value(new NumTriesRetryStrategy(numTries - 1))
    else
      Future.exception(new Exception)
  }
}
