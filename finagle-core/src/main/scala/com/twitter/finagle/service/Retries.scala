package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.param.{HighResTimer, Stats}
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.util.{Time, Try, Future}

/**
 * The [[Stack]] parameters and modules for configuring
 * '''which''' and '''how many''' failed requests are retried for
 * a client.
 */
object Retries {

  val Role = Stack.Role("Retries")

  /** The upper bound on service acquisition attempts */
  private[service] val Effort = 25

  /**
   * Determines '''which''' failed requests are eligible for
   * being retried.
   *
   * @note Currently only responses which are [[com.twitter.util.Throw Throws]]
   * are considered. These exceptions will '''not include''' application level
   * failures which is particularly important for codecs that include exceptions,
   * such as `Thrift`.
   *
   * @see [[RetryExceptionsFilter]]
   */
  private[twitter] case class Policy(retryPolicy: RetryPolicy[Try[Nothing]]) {
    def mk(): (Policy, Stack.Param[Policy]) =
      (this, Policy.param)
  }
  private[twitter] object Policy {
    implicit val param = Stack.Param(Policy(RetryPolicy.Never))
  }

  /**
   * Determines '''how many''' failed requests are eligible for
   * being retried.
   */
  case class Budget(retryBudget: RetryBudget) {
    def mk(): (Budget, Stack.Param[Budget]) =
      (this, Budget)
  }
  object Budget extends Stack.Param[Budget] {
    def default: Budget = Budget(RetryBudget())

    implicit val param: Stack.Param[Budget] = this
  }

  /**
   * A single budget needs to be shared across a [[RequeueFilter]] and
   * a [[RetryFilter]] for debiting purposes, but we only want one of
   * the calls to `RetryBudget.request()` to count. This allows for
   * swallowing the call to `request` in the second filter.
   */
  private class WithdrawOnlyRetryBudget(underlying: RetryBudget) extends RetryBudget {
    def deposit(): Unit = ()
    def tryWithdraw(): Boolean = underlying.tryWithdraw()
    def balance: Long = underlying.balance
  }

  // semi-arbitrary, but we don't want requeues to eat the entire budget
  private[this] val MaxRequeuesPerReq = 0.2

  /**
   * Retries failures that are guaranteed to be safe to retry
   * (see [[RetryPolicy.RetryableWriteException]]).
   */
  private[finagle] def moduleRequeueable[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[Stats, Budget, ServiceFactory[Req, Rep]] {
      def role: Stack.Role = Retries.Role

      def description: String =
        "Retries requests, at the service application level, that have been rejected"

      def make(
        statsP: param.Stats,
        budgetP: Budget,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val statsRecv = statsP.statsReceiver
        val scoped = statsRecv.scope("retries")
        val requeues = scoped.counter("requeues")
        val retryBudget = budgetP.retryBudget

        val filters = newRequeueFilter(retryBudget, false, scoped, next)
        svcFactory(retryBudget, filters, scoped, requeues, next)
      }
    }

  /**
   * Retries failures that are guaranteed to be safe to retry
   * (see [[RetryPolicy.RetryableWriteException]]) and a user defined [[RetryPolicy]].
   *
   * The [[RetryPolicy]] configured via [[Retries.Policy]] sets a
   * limit on per-request retries while the [[RetryBudget]] configured
   * via [[Retries.Budget]] sets a limit across multiple requests.
   *
   * @note the failures seen in the client will '''not include''' application
   *       level failures. This is particularly important for codecs that
   *       include exceptions, such as `Thrift`.
   */
  private[finagle] def moduleWithRetryPolicy[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module4[
      Stats,
      Budget,
      Policy,
      HighResTimer,
      ServiceFactory[Req, Rep]
    ] {
      def role: Stack.Role = Retries.Role

      def description: String =
        "Retries requests, at the service application level, that have been rejected " +
          "or meet the application-configured retry policy for transport level failures."

      def make(
        statsP: param.Stats,
        budgetP: Budget,
        policyP: Policy,
        timerP: HighResTimer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val statsRecv = statsP.statsReceiver
        val scoped = statsRecv.scope("retries")
        val requeues = scoped.counter("requeues")
        val retryBudget = budgetP.retryBudget
        val retryPolicy = policyP.retryPolicy

        val filters =
          if (retryPolicy eq RetryPolicy.Never) {
            newRequeueFilter(retryBudget, false, scoped, next)
          } else {
            val retryFilter = new RetryExceptionsFilter[Req, Rep](
              retryPolicy, timerP.timer, statsRecv, retryBudget)
            // note that we wrap the budget, since the retry filter wraps this
            val requeueFilter = newRequeueFilter(retryBudget, true, scoped, next)
            retryFilter.andThen(requeueFilter)
          }

        svcFactory(retryBudget, filters, scoped, requeues, next)
      }
    }

  private[this] def newRequeueFilter[Req, Rep](
    retryBudget: RetryBudget,
    withdrawsOnly: Boolean,
    statsReceiver: StatsReceiver,
    next: ServiceFactory[Req, Rep]
  ): RequeueFilter[Req, Rep] = {
    val budget =
      if (withdrawsOnly) new WithdrawOnlyRetryBudget(retryBudget)
      else retryBudget
    new RequeueFilter[Req, Rep](
      budget,
      statsReceiver,
      // TODO: If we ensure that the stack doesn't return restartable
      // failures when it isn't Open, we wouldn't need to gate on status.
      () => next.status == Status.Open,
      MaxRequeuesPerReq)
  }

  private[this] def svcFactory[Req, Rep](
    retryBudget: RetryBudget,
    filters: Filter[Req, Rep, Req, Rep],
    statsReceiver: StatsReceiver,
    requeuesCounter: Counter,
    next: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] = {
    new ServiceFactoryProxy(next) {
      // We define the gauge inside of the ServiceFactory so that their lifetimes
      // are tied together.
      private[this] val budgetGauge =
        statsReceiver.addGauge("budget") { retryBudget.balance }

      private[this] val serviceFn: Service[Req, Rep] => Service[Req, Rep] =
        service => filters.andThen(service)

      /**
       * Failures to acquire a service can be thought of as local failures because
       * we're certain that we haven't dispatched a request yet. Thus, this simply
       * tries up to `n` attempts to acquire a service. However, we still only
       * requeue a subset of exceptions (currently only `RetryableWriteExceptions`) as
       * some exceptions to acquire a service are considered fatal.
       */
      private[this] def applySelf(conn: ClientConnection, n: Int): Future[Service[Req, Rep]] =
        self(conn).rescue {
          case RetryPolicy.RetryableWriteException(_) if n > 0 && status == Status.Open =>
            requeuesCounter.incr()
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
        applySelf(conn, Effort).map(serviceFn)

      override def close(deadline: Time): Future[Unit] = {
        budgetGauge.remove()
        self.close(deadline)
      }

    }
  }

}
