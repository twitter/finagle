package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.param.Stats
import com.twitter.finagle.param.{ResponseClassifier => ParamResponseClassifier}
import com.twitter.finagle.stats.Counter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util._

/**
 * The [[Stack]] parameters and modules for configuring
 * '''which''' and '''how many''' failed requests are retried for
 * a client.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Servers.html#request-timeout user guide]]
 *      for more details.
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
   *
   * @param retryBudget maintains a budget of remaining retries for
   *                    an individual request.
   *
   * @param requeueBackoffs schedule of delays applied between each
   *                        automatic retry.
   * @note requeueBackoffs only apply to automatic retries and not to
   *       requests using a [[RetryPolicy]]
   */
  case class Budget(
    retryBudget: RetryBudget,
    requeueBackoffs: Backoff = Budget.emptyBackoffSchedule) {
    def this(retryBudget: RetryBudget) =
      this(retryBudget, Budget.emptyBackoffSchedule)

    def mk(): (Budget, Stack.Param[Budget]) =
      (this, Budget)

    def canEqual(other: Any): Boolean = other.isInstanceOf[Budget]

    /**
     * The equals method only compares the retryBudget field of this instance,
     * since the requeueBackoffs Stream[Duration] is possibly infinite,
     * resulting in an infinite computation to compare for equality.
     */
    override def equals(other: Any): Boolean = other match {
      case that: Budget =>
        that.canEqual(this) && retryBudget == that.retryBudget
      case _ => false
    }

    /**
     * For the hashCode of this class we only take into account the hashCode of the RetryBudget field.
     * The requeueBackoffs Stream[Duration] is possibly infinite,
     * resulting in an infinite computation to get to the hashCode.
     */
    override def hashCode(): Int =
      retryBudget.##
  }

  object Budget extends Stack.Param[Budget] {

    /**
     * Default backoff stream to use for automatic retries.
     * All Zeros.
     */
    val emptyBackoffSchedule = Backoff.const(Duration.Zero)

    def default: Budget = Budget(RetryBudget(), emptyBackoffSchedule)

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
  def moduleRequeueable[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      def role: Stack.Role = Retries.Role

      def description: String =
        "Retries requests, at the service application level, that have been rejected"

      val parameters = Seq(
        implicitly[Stack.Param[Stats]],
        implicitly[Stack.Param[Budget]],
        implicitly[Stack.Param[HighResTimer]],
        implicitly[Stack.Param[ParamResponseClassifier]]
      )

      def make(
        params: Stack.Params,
        next: Stack[ServiceFactory[Req, Rep]]
      ): Stack[ServiceFactory[Req, Rep]] = {
        val statsRecv = params[Stats].statsReceiver
        val scoped = statsRecv.scope("retries")
        val requeues = scoped.counter("requeues")
        val budget = params[Budget]
        val timer = params[HighResTimer].timer
        val classifier = params[ParamResponseClassifier].responseClassifier

        // Filters/factories lower in the stack may also make use of the [[RetryBudget]] param.
        // However, if this param is not configured explicitly, the default is used, which creates
        // a new, unshared [[RetryBudget]]. In order to have a per-client budget shared across
        // retry modules and all modules lower in the stack, we explicitly configure the param
        // here.
        val nextSvcFac = next.make(params + budget)

        val filters = newRequeueFilter[Req, Rep](
          budget.retryBudget,
          budget.requeueBackoffs,
          withdrawsOnly = false,
          scoped,
          timer,
          classifier
        )

        Stack.leaf(this, svcFactory(budget.retryBudget, filters, scoped, requeues, nextSvcFac))
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
    new Stack.Module5[
      Stats,
      Budget,
      Policy,
      HighResTimer,
      ParamResponseClassifier,
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
        responseClassifier: ParamResponseClassifier,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val statsRecv = statsP.statsReceiver
        val scoped = statsRecv.scope("retries")
        val requeues = scoped.counter("requeues")
        val retryBudget = budgetP.retryBudget
        val retryPolicy = policyP.retryPolicy
        val classifier = responseClassifier.responseClassifier

        val filters =
          if (retryPolicy eq RetryPolicy.Never) {
            newRequeueFilter[Req, Rep](
              retryBudget,
              budgetP.requeueBackoffs,
              withdrawsOnly = false,
              scoped,
              timerP.timer,
              classifier
            )
          } else {
            val retryFilter =
              new RetryExceptionsFilter[Req, Rep](retryPolicy, timerP.timer, statsRecv, retryBudget)
            // note that we wrap the budget, since the retry filter wraps this
            val requeueFilter = newRequeueFilter[Req, Rep](
              retryBudget,
              budgetP.requeueBackoffs,
              withdrawsOnly = true,
              scoped,
              timerP.timer,
              classifier
            )
            retryFilter.andThen(requeueFilter)
          }

        svcFactory(retryBudget, filters, scoped, requeues, next)
      }
    }

  private[this] def newRequeueFilter[Req, Rep](
    retryBudget: RetryBudget,
    retrySchedule: Backoff,
    withdrawsOnly: Boolean,
    statsReceiver: StatsReceiver,
    timer: Timer,
    classifier: ResponseClassifier
  ): RequeueFilter[Req, Rep] = {
    val budget =
      if (withdrawsOnly) new WithdrawOnlyRetryBudget(retryBudget)
      else retryBudget
    new RequeueFilter[Req, Rep](
      budget,
      retrySchedule,
      MaxRequeuesPerReq,
      classifier,
      statsReceiver,
      timer)
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
      private[this] val notOpenCounter =
        statsReceiver.counter("not_open")

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
        self(conn).transform {
          case Throw(e @ RetryPolicy.RetryableWriteException(_)) if n > 0 =>
            if (status == Status.Open) {
              requeuesCounter.incr()
              applySelf(conn, n - 1)
            } else {
              notOpenCounter.incr()
              Future.exception(e)
            }

          // this indicates that there isn't a problem with the remote peer but
          // the returned service is unusable, which allows a protocol
          // to point out when a connection is dead for a reason that shouldn't
          // trigger circuit breaking
          case Return(deadSvc) if deadSvc.status == Status.Closed && n > 0 =>
            // If `this` is in an Open state we're stopping `deadSvc` from propagating up the stack
            // to either an application or FactoryToService wrapper and since it's not part of the Service
            // contract that Status can only be Closed when the close method was called, we must manually
            // close the session to forestall resource leaks.
            //
            // However, `deadSvc` will be propagated up the stack if `this` is not in an Open state.
            // In this case, we do not close the session here
            if (status == Status.Open) {
              deadSvc.close()
              requeuesCounter.incr()
              applySelf(conn, n - 1)
            } else {
              notOpenCounter.incr()
              Future.value(deadSvc)
            }

          case t => Future.const(t)
        }

      /**
       * Note: We attempt service acquisition with a fixed budget (i.e. `Effort`).
       *
       * Clients built with `newService` compose `FactoryToService` as part of their stack,
       * making service acquisition part of *each* service application,
       * so we requeue service acquisition up to `Effort` times for each service application,
       * followed by request requeues as per [[RequeueFilter]].
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
