package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.TokenBucket
import com.twitter.util.Future

private[finagle] object Requeues {
  val role = Stack.Role("Requeues")

  /** Cost determines the relative cost of a reissue vs. an initial issue. */
  val Cost = 5

  /** The upper bound on service acquisition attempts */
  val Effort = 25

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = Requeues.role
      val description = "Requeue requests that have been rejected at the service application level"

      def make(stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(sr0) = stats
        val sr = sr0.scope("requeue")
        val requeues = sr.counter("requeues")

        // Each window in the token bucket has a reserve of 500 tokens, which allows for
        // 10 free reissues/s. This is to aid just-starting or low-velocity clients.
        val bucket = TokenBucket.newLeakyBucket(ttl = 10.seconds, reserve = Cost*10*10)

        // The filter manages the tokens in the bucket: it credits a token per request and
        // debits `Cost` per reqeueue. Thus, with the current parameters, reissue can never
        // exceed the greater of 10/s and ~15% of total issue.
        val requeueFilter = new RequeueFilter[Req, Rep](bucket, Cost, sr, () => next.status)

        new ServiceFactoryProxy(next) {
          // We define the gauge inside of the ServiceFactory so that their lifetimes
          // are tied together.
          private[this] val budgetGauge = sr.addGauge("budget") { bucket.count/Cost }

          /**
            * Failures to acquire a service can be thought of as local failures because
            * we're certain that we haven't dispatched a request yet. Thus, this simply
            * tries up to `n` attempts to acquire a service. However, we still only
            * requeue a subset of exceptions (currently only `RetryableWriteExceptions`) as
            * some exceptions to acquire a service are considered fatal.
            */
          private[this] def applySelf(conn: ClientConnection, n: Int): Future[Service[Req, Rep]] =
            self(conn).rescue {
              case RetryPolicy.RetryableWriteException(_) if status == Status.Open && n > 0 =>
                requeues.incr()
                applySelf(conn, n-1)
            }

          /**
            * Note: This may seem like we are always attempting service acquisition
            * with a fixed budget (i.e. `Effort`). However, this is not always the case
            * and is dependent on how the client is built (i.e. `newService`/`newClient`).
            *
            * Clients built with `newService` compose FactoryToService as part of their stack
            * which effectively moves service acquisition as part of service application,
            * so all requeues are gated by [[RequeueFilter]].
            *
            * Clients built with `newClient` separate requeues into two distinct phases for
            * service acquisition and service application. First, we try up to `Effort` to acquire
            * a new service. Then we requeue requests as per [[RequeueFilter]]. Note, because the
            * `newClient` API gives the user control over which service (i.e. session) to issue a
            * request, request level requeues using this API must be issued over the same service.
            *
            * See StackClient#newStack for more details.
            */
          override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
            applySelf(conn, Effort).map(service => requeueFilter andThen service)
        }
      }
    }
}

/**
  * Requeues service application failures that are encountered in modules below it.
  * In addition to requeueing local failures, the filter re-issues remote requests
  * that have been NACKd. The policy is inherited from `RetryPolicy.RetryableWriteException`.
  * Requeues are also rate-limited according to our budget in `bucket`. They are credited by
  * the token bucket at a 1:`requeueCost` ratio of requests. That is, requests are credited 1
  * token and requeues are debited `requeueCost` tokens.
  *
  * @param bucket Maintains our requeue budget.
  *
  * @param requeueCost How much to debit from `bucket` per each requeue.
  *
  * @param statsReceiver for stats reporting.
  *
  * @param stackStatus Represents the status of the stack which generated the
  * underlying service. Requeues are only dispatched when this status reports
  * Status.Open.
  */
private[finagle] class RequeueFilter[Req, Rep](
    bucket: TokenBucket,
    requeueCost: Int,
    statsReceiver: StatsReceiver,
    stackStatus: () => Status)
  extends SimpleFilter[Req, Rep] {

  private[this] val requeueCounter = statsReceiver.counter("requeues")
  private[this] val budgetExhaustCounter = statsReceiver.counter("budget_exhausted")

  private[this] def applyService(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    service(req).rescue {
      case exc@RetryPolicy.RetryableWriteException(_) =>
        // TODO: If we ensure that the stack doesn't return restartable
        // failures when it isn't Open, we wouldn't need to gate on status.
        if (stackStatus() != Status.Open) {
          Future.exception(exc)
        } else if (bucket.tryGet(requeueCost)) {
          requeueCounter.incr()
          applyService(req, service)
        } else {
          budgetExhaustCounter.incr()
          Future.exception(exc)
        }
    }
  }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    bucket.put(1)
    applyService(req, service)
  }
}
