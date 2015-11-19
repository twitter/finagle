package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Timer, Duration, Future}

/**
 * Requeues service application failures that are encountered in modules below it.
 * In addition to requeueing local failures, the filter re-issues remote requests
 * that have been NACKd. The policy is inherited from `RetryPolicy.RetryableWriteException`.
 * Requeues are also rate-limited according to our retry budget in the [[RetryBudget]].
 *
 * @param retryBudget Maintains our requeue budget.
 *
 * @param retryBackoffs Stream of backoffs to use before each retry. (e.g. the
 *                      first element is used to delay the first retry, 2nd for
 *                      the second retry and so on)
 *
 * @param statsReceiver for stats reporting, typically scoped to ".../retries/"
 *
 * @param canRetry Represents whether or not it is appropriate to issue a
 * retry. This is separate from `retryBudget`.
 *
 * @param maxRetriesPerReq The maximum number of retries to make for a given request
 * computed as a percentage of `retryBudget.balance`.
 * Used to prevent a single request from using up a disproportionate amount of the budget.
 * Must be non-negative.
 *
 * @param timer Timer used to schedule retries
 * @note consider using a [[Timer]] with high resolution so there is
 * less correlation between retries. For example [[HighResTimer.Default]]
 */
private[finagle] class RequeueFilter[Req, Rep](
    retryBudget: RetryBudget,
    retryBackoffs: Stream[Duration],
    statsReceiver: StatsReceiver,
    canRetry: () => Boolean,
    maxRetriesPerReq: Double,
    timer: Timer)
  extends SimpleFilter[Req, Rep] {

  require(maxRetriesPerReq >= 0,
    s"maxRetriesPerReq must be non-negative: $maxRetriesPerReq")

  private[this] val requeueCounter = statsReceiver.counter("requeues")
  private[this] val budgetExhaustCounter = statsReceiver.counter("budget_exhausted")

  private[this] def applyService(
    req: Req,
    service: Service[Req, Rep],
    retriesRemaining: Int,
    backoffs: Stream[Duration]
  ): Future[Rep] = {
    service(req).rescue {
      case exc@RetryPolicy.RetryableWriteException(_) =>
        if (!canRetry()) {
          Future.exception(exc)
        } else if (retriesRemaining > 0 && retryBudget.tryWithdraw()) {
          backoffs match {
            case Duration.Zero #:: rest =>
              // no delay between retries. Retry immediately.
              requeueCounter.incr()
              applyService(req, service, retriesRemaining - 1, rest)
            case delay #:: rest =>
              // Delay and then retry.
              timer.doLater(delay) {
                requeueCounter.incr()
                applyService(req, service, retriesRemaining - 1, rest)
              }.flatten
            case _ =>
              // Schedule has run out of entries. Budget is empty.
              budgetExhaustCounter.incr()
              Future.exception(exc)
          }
        } else {
          if (retriesRemaining > 0)
            budgetExhaustCounter.incr()
          Future.exception(exc)
        }
    }
  }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    retryBudget.deposit()
    val maxRetries = Math.ceil(maxRetriesPerReq * retryBudget.balance).toInt
    applyService(req, service, maxRetries, retryBackoffs)
  }
}
