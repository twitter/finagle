package com.twitter.finagle.channel

import org.jboss.netty.util.HashedWheelTimer
import com.twitter.finagle.util.Conversions._
import com.twitter.util._
import com.twitter.finagle.util.Timer

object RetryingBroker {
  def tries(underlying: Broker, numTries: Int) =
    new RetryingBroker(underlying, new NumTriesRetryStrategy(numTries))
  def exponential(underlying: Broker, initial: Duration, multiplier: Int) =
    new RetryingBroker(underlying, new ExponentialBackoffRetryStrategy(initial, multiplier))
}

trait RetryStrategy {
  def apply(): Future[RetryStrategy]
}

/**
 * The RetryingBroker attempts to replay a request in certain failure
 * conditions.  Currently we define *any* WriteError as a retriable
 * error, but nothing else. We cannot make broader assumptions without
 * application knowledge (eg. the request may be side effecting),
 * except to say that the message itself was not delivered. Any
 * other types of retries must be done in the service stack.
 */
class RetryingBroker(val underlying: Broker, retryStrategy: RetryStrategy) extends WrappingBroker {
  override def apply(request: AnyRef) = {
    val promise = new Promise[AnyRef]
    dispatch(request, promise, retryStrategy())
    promise
  }

  private[this] def dispatch(
      request: AnyRef,
      promise: Promise[AnyRef],
      strategy: Future[RetryStrategy]) {
    underlying(request) respond {
      // Only write exceptions are retriable.
      case t@Throw(cause) if cause.isInstanceOf[WriteException] =>
        // Time to retry.
        strategy respond {
          case Return(nextStrategy) =>
            dispatch(request, promise, nextStrategy())
          case Throw(_) =>
            promise.updateIfEmpty(t)
        }

      case rv@_ => promise.updateIfEmpty(rv)
    }
  }
}

class NumTriesRetryStrategy(numTries: Int) extends RetryStrategy {
  def apply() = {
    // A retry strategy is invoked only after failure. So the total
    // number of tries need to be bumped by one.
    if (numTries > 1)
      Future.value(new NumTriesRetryStrategy(numTries - 1))
    else
      Future.exception(new Exception)
  }
}

class ExponentialBackoffRetryStrategy(
  delay: Duration,
  multiplier: Int,
  timer: Timer = Timer.default)
  extends RetryStrategy
{
  def apply() = {
    val future = new Promise[RetryStrategy]

    timer.schedule(delay) {
      future() = Return(
        new ExponentialBackoffRetryStrategy(delay * multiplier, multiplier))
    }

    future
  }
}
