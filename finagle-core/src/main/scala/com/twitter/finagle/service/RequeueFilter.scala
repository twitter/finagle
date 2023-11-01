package com.twitter.finagle.service

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.context
import com.twitter.finagle._
import com.twitter.util._

/**
 * Requeues service application failures that are encountered in modules below it.
 * In addition to requeueing local failures, the filter re-issues remote requests
 * that have been NACKd. The policy is inherited from `RetryPolicy.RetryableWriteException`.
 * Requeues are also rate-limited according to our retry budget in the [[RetryBudget]].
 *
 * @param retryBudget Maintains our requeue budget.
 *
 * @param retryBackoffs a policy encoded [[Backoff]] to use before each retry. (e.g. the
 *                      first element is used to delay the first retry, 2nd for
 *                      the second retry and so on)
 *
 * @param responseClassifier for determining which responses qualify as "successful" for
 *                           the purposes of incrementing our retryBudget.
 *
 * @param maxRetriesPerReq The maximum number of retries to make for a given request
 * computed as a percentage of `retryBudget.balance`.
 * Used to prevent a single request from using up a disproportionate amount of the budget.
 * Must be non-negative.
 *
 * @param statsReceiver for stats reporting, typically scoped to ".../retries/"
 *
 * @param timer Timer used to schedule retries
 *
 * @note consider using a [[Timer]] with high resolution so there is
 * less correlation between retries. For example [[com.twitter.finagle.util.DefaultTimer]]
 *
 * @see The [[https://twitter.github.io/finagle/guide/Servers.html#request-timeout user guide]]
 *      for more details.
 */
private[finagle] class RequeueFilter[Req, Rep](
  retryBudget: RetryBudget,
  retryBackoffs: Backoff,
  maxRetriesPerReq: Double,
  responseClassifier: ResponseClassifier,
  statsReceiver: StatsReceiver,
  timer: Timer)
    extends SimpleFilter[Req, Rep] {
  import RequeueFilter._

  require(maxRetriesPerReq >= 0, s"maxRetriesPerReq must be non-negative: $maxRetriesPerReq")

  private[this] val requeueCounter = statsReceiver.counter("requeues")
  private[this] val budgetExhaustCounter = statsReceiver.counter("budget_exhausted")
  private[this] val requestLimitCounter = statsReceiver.counter("request_limit")
  private[this] val requeueStat = statsReceiver.stat("requeues_per_request")
  private[this] val canNotRetryCounter = statsReceiver.counter("cannot_retry")

  private[this] def responseFuture(attempt: Int, t: Try[Rep]): Future[Rep] = {
    requeueStat.add(attempt)
    Future.const(t)
  }

  private[this] def issueRequest(
    req: Req,
    service: Service[Req, Rep],
    attempt: Int,
    retriesRemaining: Int,
    backoffs: Backoff
  ): Future[Rep] = {
    Contexts.broadcast.let(context.Requeues, context.Requeues(attempt)) {
      val trace = Trace()
      val shouldTrace = attempt > 0 && trace.isActivelyTracing
      if (shouldTrace) {
        trace.record(RequeuedAnnotation)
        trace.record("clnt/requeue_begin")
        trace.recordBinary("clnt/requeue_attempt", attempt)

        // we set the rpc name to "requeue". The true rpc name can be inferred by
        // the recorded parent span
        trace.recordRpc("requeue")
      }

      val svcRep = service(req)
      if (shouldTrace) {
        svcRep.ensure(trace.record("clnt/requeue_end"))
      }

      svcRep.transform {
        case t @ Throw(Requeueable(cause)) =>
          // we always trace the exception
          if (trace.isActivelyTracing) {
            trace.recordBinary("clnt/requeue_exc", s"${cause.getClass.getName}:${cause.getMessage}")
          }

          // We check the service's status to determine if a retry should be issued.
          // The status reflects the resources available, depending on the stack
          // configuration which is protocol specific. This could be all available
          // endpoints or a single session
          if (service.status != Status.Open) {
            canNotRetryCounter.incr()
            responseFuture(attempt, t)
          } else if (retriesRemaining > 0 && retryBudget.tryWithdraw()) {
            if (backoffs.isExhausted) {
              // Schedule has run out of entries. Budget is empty.
              budgetExhaustCounter.incr()
              responseFuture(attempt, t).transform(FailureFlags.asNonRetryable)
            } else if (backoffs.duration == Duration.Zero) {
              // no delay between retries. Retry immediately.
              requeueCounter.incr()
              applyService(req, service, attempt + 1, retriesRemaining - 1, backoffs.next)
            } else {
              // Delay and then retry.
              timer
                .doLater(backoffs.duration) {
                  requeueCounter.incr()
                  applyService(req, service, attempt + 1, retriesRemaining - 1, backoffs.next)
                }
                .flatten
            }
          } else {
            if (retriesRemaining > 0)
              budgetExhaustCounter.incr()
            else
              requestLimitCounter.incr()
            responseFuture(attempt, t).transform(FailureFlags.asNonRetryable)
          }
        case t =>
          responseFuture(attempt, t)
      }
    }
  }

  private[this] def applyService(
    req: Req,
    service: Service[Req, Rep],
    attempt: Int,
    retriesRemaining: Int,
    backoffs: Backoff
  ): Future[Rep] = {
    // If we've requeued a request, `attempt > 0`, we want the child request to subsequently
    // generate a new spanId for this request. The original request, `attempt == 0` should
    // retain the original span
    if (attempt > 0) {
      val requeueTraceId: TraceId = Trace.nextId
      Trace.letId(requeueTraceId) {
        issueRequest(req, service, attempt, retriesRemaining, backoffs)
      }
    } else {
      issueRequest(req, service, attempt, retriesRemaining, backoffs)
    }
  }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val maxRetries = Math.ceil(maxRetriesPerReq * retryBudget.balance).toInt
    applyService(req, service, 0, maxRetries, retryBackoffs).respond { rep: Try[Rep] =>
      // Ignorables are never safe to retry, so for our
      // purposes here, we don't increment the retry budget.
      val shouldDeposit =
        responseClassifier.applyOrElse(ReqRep(req, rep), ResponseClassifier.Default) match {
          case ResponseClass.Successful(_) => true
          case ResponseClass.Failed(_) => false
          case ResponseClass.Ignorable => false
        }
      if (shouldDeposit) { retryBudget.deposit() }
    }
  }
}

object RequeueFilter {
  private val RequeuedAnnotation = Annotation.Message("Requeued Request")

  /**
   * An extractor for exceptions which are known to be safe to retry.
   */
  object Requeueable {
    def unapply(t: Throwable): Option[Throwable] =
      RetryPolicy.RetryableWriteException.unapply(t)
  }

}
