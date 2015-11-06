package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future

/**
 * Requeues service application failures that are encountered in modules below it.
 * In addition to requeueing local failures, the filter re-issues remote requests
 * that have been NACKd. The policy is inherited from `RetryPolicy.RetryableWriteException`.
 * Requeues are also rate-limited according to our retry budget in the [[RetryBudget]].
 *
 * @param retryBudget Maintains our requeue budget.
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
 */
private[finagle] class RequeueFilter[Req, Rep](
    retryBudget: RetryBudget,
    statsReceiver: StatsReceiver,
    canRetry: () => Boolean,
    maxRetriesPerReq: Double)
  extends SimpleFilter[Req, Rep] {

  require(maxRetriesPerReq >= 0,
    s"maxRetriesPerReq must be non-negative: $maxRetriesPerReq")

  private[this] val requeueCounter = statsReceiver.counter("requeues")
  private[this] val budgetExhaustCounter = statsReceiver.counter("budget_exhausted")

  private[this] def applyService(
    req: Req,
    service: Service[Req, Rep],
    retriesRemaining: Int
  ): Future[Rep] = {
    service(req).rescue {
      case exc@RetryPolicy.RetryableWriteException(_) =>
        if (!canRetry()) {
          Future.exception(exc)
        } else if (retriesRemaining > 0 && retryBudget.tryWithdraw()) {
          requeueCounter.incr()
          applyService(req, service, retriesRemaining - 1)
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
    applyService(req, service, maxRetries)
  }
}
