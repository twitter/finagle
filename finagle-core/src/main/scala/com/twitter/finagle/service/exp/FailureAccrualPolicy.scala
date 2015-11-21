package com.twitter.finagle.service.exp

import com.twitter.conversions.time._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.Ema
import com.twitter.util.{Duration, Stopwatch}
import java.util.Arrays
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

/**
 * A `FailureAccrualPolicy` is used by `FailureAccrualFactory` to determine
 * whether to mark an endpoint dead upon a request failure. On each successful
 * response, `FailureAccrualFactory` calls `recordSuccess()`. On each failure,
 * `FailureAccrualFactory` calls `markDeadOnFailure()` to obtain the duration
 * to mark the endpoint dead for; (Some(Duration)), or None.
 */
trait FailureAccrualPolicy {

  /** Invoked by FailureAccrualFactory when a request is successful. */
  def recordSuccess(): Unit

  /**
   * Invoked by FailureAccrualFactory when a non-probing request fails.
   * If it returns Some(Duration), the FailureAccrualFactory will mark the
   * endpoint dead for the specified Duration.
   */
  def markDeadOnFailure(): Option[Duration]

  /**
   * Invoked by FailureAccrualFactory when an endpoint is revived after
   * probing. Used to reset any history.
   */
  def revived(): Unit
}

object FailureAccrualPolicy {

  private[this] val Success = 1
  private[this] val Failure = 0

  private[this] val constantBackoff = Backoff.const(300.seconds)

  /**
   * A policy based on an exponentially-weighted moving average success rate
   * over a window of requests. A moving average is used so the success rate
   * calculation is biased towards more recent requests; for an endpoint with
   * low traffic, the window will span a longer time period and early successes
   * and failures may not accurately reflect the current health.
   *
   * If the computed weighted success rate is less
   * than the required success rate, `markDeadOnFailure()` will return
   * Some(Duration).
   *
   * @see com.twitter.finagle.util.Ema for how the succes rate is computed
   *
   * @param requiredSuccessRate successRate that must be met
   *
   * @param window window over which the success rate is tracked. `window` requests
   * must occur for `markDeadOnFailure()` to ever return Some(Duration)
   *
   * @param markDeadFor stream of durations to use for the next duration
   * returned from `markDeadOnFailure()`
   */
  def successRate(
    requiredSuccessRate: Double,
    window: Int,
    markDeadFor: Stream[Duration]
  ): FailureAccrualPolicy = new FailureAccrualPolicy {

    // Pad the back of the stream to mark dead for a constant amount (300 seconds)
    // when the stream runs out.
    private[this] val freshMarkDeadFor = markDeadFor ++ constantBackoff

    // The head of `nextMarkDeadFor` is the next duration that will be returned
    // from `markDeadOnFailure()` if the required success rate is not met.
    // The tail is the remainder of the durations.
    private[this] var nextMarkDeadFor = freshMarkDeadFor

    private[this] var totalRequests = 0L
    private[this] val successRate = new Ema(window)

    def recordSuccess(): Unit = synchronized {
      totalRequests += 1
      successRate.update(totalRequests, Success)
    }

    def markDeadOnFailure(): Option[Duration] = synchronized {
      totalRequests += 1
      val sr = successRate.update(totalRequests, Failure)
      if (totalRequests >= window  && sr < requiredSuccessRate) {
        val duration = nextMarkDeadFor.head
        nextMarkDeadFor = nextMarkDeadFor.tail
        Some(duration)
      } else {
        None
      }
    }

    def revived(): Unit = synchronized {
      nextMarkDeadFor = freshMarkDeadFor
      successRate.reset()
    }
  }

  /**
   * A policy based on a maximum number of consecutive failures. If `numFailures`
   * occur consecutively, `checkFailures()` will return a Some(Duration) to
   * mark an endpoint dead for.
   *
   * @param numFailures number of consecutive failures
   *
   * @param markDeadFor stream of durations to use for the next duration
   * returned from `markDeadOnFailure()`
   */
  def consecutiveFailures(
    numFailures: Int,
    markDeadFor: Stream[Duration]
  ): FailureAccrualPolicy = new FailureAccrualPolicy {

    // Pad the back of the stream to mark dead for a constant amount (300 seconds)
    // when the stream runs out.
    private[this] val freshMarkDeadFor = markDeadFor ++ constantBackoff

    // The head of `nextMarkDeadFor` is the next duration that will be returned
    // from `markDeadOnFailure()` if the required success rate is not met.
    // The tail is the remainder of the durations.
    private[this] var nextMarkDeadFor = freshMarkDeadFor

    private[this] var consecutiveFailures = 0L

    def recordSuccess(): Unit = synchronized {
      consecutiveFailures = 0
    }

    def markDeadOnFailure(): Option[Duration] = synchronized {
      consecutiveFailures += 1
      if (consecutiveFailures >= numFailures) {
        val duration = nextMarkDeadFor.head
        nextMarkDeadFor = nextMarkDeadFor.tail
        Some(duration)
      } else {
        None
      }
    }

    def revived(): Unit = synchronized {
      consecutiveFailures = 0
      nextMarkDeadFor = freshMarkDeadFor
    }
  }
}
