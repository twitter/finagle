package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier, Retries, RetryBudget}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.WindowedPercentileHistogram
import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.util.tunable.Tunable

object BackupRequestFilter {
  val role = Stack.Role("BackupRequestFilter")
  val description = "Send a backup request at a configurable latency"

  /**
   * Sentinel [[Throwable]] when the original request does not complete within the timeout
   */
  private val OrigRequestTimeout = Failure("Original request did not complete in time")

  private val SupersededRequestFailure = Failure.ignorable(
    "Request was superseded by another in BackupRequestFilter")

  private val log = Logger.get(this.getClass.getName)

  // Refresh rate for refreshing the configured percentile from the [[WindowedPercentile]].
  private val RefreshPercentileInterval = 3.seconds

  private def getAndValidateMaxExtraLoad(maxExtraLoad: Tunable[Double]): Double =
    maxExtraLoad() match {
      case Some(maxExtraLoad) if (maxExtraLoad >= 0.0 && maxExtraLoad < 1.0) => maxExtraLoad
      case Some(invalidMaxExtraLoad) => log.error(
        s"maxExtraLoad must be between 0.0 and 1.0, was $invalidMaxExtraLoad. Using 0.0")
        0.0
      case None => 0.0
    }

  private[client] def newRetryBudget(maxExtraLoad: Double, nowMillis: () => Long): RetryBudget =
    if (maxExtraLoad == 0.0) RetryBudget.Empty
    else
      RetryBudget(
        ttl = RetryBudget.DefaultTtl,
        minRetriesPerSec = RetryBudget.DefaultMinRetriesPerSec,
        percentCanRetry = maxExtraLoad,
        nowMillis)

  sealed trait Param {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }

  object Param {

    private[client] case class Configured(
        maxExtraLoad: Tunable[Double],
        sendInterrupts: Boolean)
      extends Param
    case object Disabled extends Param
    implicit val param: Stack.Param[BackupRequestFilter.Param] = Stack.Param(Disabled)
  }

  /**
   * Configuration to disable [[BackupRequestFilter]]
   */
  val Disabled: Param = Param.Disabled

  /**
   * Configure [[BackupRequestFilter]].
   *
   * @param maxExtraLoad How much extra load, as a fraction, we are willing to send to the server.
   *                  Must be between 0.0 and 1.0.
   *
   * @param sendInterrupts Whether or not to interrupt the original or backup request when a response
   *                       is returned and the result of the outstanding request is superseded. For
   *                       protocols without a control plane, where the connection is cut on
   *                       interrupts, this should be "false" to avoid connection churn.
   */
  def Configured(maxExtraLoad: Double, sendInterrupts: Boolean): Param = {
    require(
      maxExtraLoad >= 0 && maxExtraLoad < 1.0,
      s"maxExtraLoad must be between 0.0 and 1.0, was $maxExtraLoad")
    Param.Configured(Tunable.const(role.name, maxExtraLoad), sendInterrupts)
  }

  def Configured(maxExtraLoad: Tunable[Double], sendInterrupts: Boolean): Param =
    Param.Configured(maxExtraLoad, sendInterrupts)

  private[this] def mkFilterFromParams[Req, Rep](
    maxExtraLoad: Tunable[Double],
    sendInterrupts: Boolean,
    params: Stack.Params
  ): BackupRequestFilter[Req, Rep] =
    new BackupRequestFilter[Req, Rep](
      maxExtraLoad,
      sendInterrupts,
      params[param.ResponseClassifier].responseClassifier,
      params[Retries.Budget].retryBudget,
      params[param.Stats].statsReceiver.scope("backups"),
      params[param.Timer].timer
    )

  /**
   * Returns `service` with a [[BackupRequestFilter]] prepended, according to the configuration
   * params in `params`. If the [[BackupRequestFilter]] has not been configured, returns the
   * same `service`.
   *
   * Users should only use this method for filtering generic services; otherwise,
   * usage through the `idempotent` method on [[MethodBuilder]] implementations is preferred.
   */
  def filterService[Req, Rep](
    params: Stack.Params,
    service: Service[Req, Rep]
  ): Service[Req, Rep] =
    params[BackupRequestFilter.Param] match {
      case BackupRequestFilter.Param.Configured(maxExtraLoad, sendInterrupts) =>
        val brf = mkFilterFromParams[Req, Rep](maxExtraLoad, sendInterrupts, params)
        new ServiceProxy[Req, Rep](brf.andThen(service)) {
          override def close(deadline: Time): Future[Unit] =
            service.close(deadline).before(brf.close(deadline))
        }
      case BackupRequestFilter.Param.Disabled =>
        service
    }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      val role = BackupRequestFilter.role
      val description = BackupRequestFilter.description

      override def parameters: Seq[Stack.Param[_]] = Seq(
        implicitly[Stack.Param[param.Stats]],
        implicitly[Stack.Param[param.Timer]],
        implicitly[Stack.Param[param.ResponseClassifier]],
        implicitly[Stack.Param[Retries.Budget]],
        implicitly[Stack.Param[BackupRequestFilter.Param]]
      )

      def make(params: Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        params[BackupRequestFilter.Param] match {
          case Param.Configured(maxExtraLoad, sendInterrupts) =>
            new BackupRequestFactory[Req, Rep](
              next,
              mkFilterFromParams(maxExtraLoad, sendInterrupts, params)
            )
          case Param.Disabled =>
            next
        }
      }
    }
}

private[client] class BackupRequestFactory[Req, Rep](
    underlying: ServiceFactory[Req, Rep],
    filter: BackupRequestFilter[Req, Rep])
  extends ServiceFactoryProxy[Req, Rep](underlying) {

  private[this] val applyBrf: Service[Req, Rep] => Service[Req, Rep] = svc =>
    filter.andThen(svc)

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    underlying(conn).map(applyBrf)

  override def close(deadline: Time): Future[Unit] =
    underlying.close(deadline).before(filter.close(deadline))
}

/**
 * This filter sends a second, "backup" request if a response for the original request has not
 * been received within some duration. This duration is derived from the configured `maxExtraLoad`;
 * it is the nth percentile latency of requests, where n is 100 * (1  - `maxExtraLoad`). For example,
 * if `maxExtraLoad` is 0.01, a backup request is sent at the p99 latency.
 *
 * Latency is calculated using a history of requests. In order to protect the backend from
 * excessive backup requests should the latency shift suddenly, a [[RetryBudget]] based on
 * `maxExtraLoad` is used. When determining whether or not to send a backup, this local budget
 * is combined with the budget configured with [[Retries.Budget]]; this means that the backend will
 * not receive more extra load than that permitted by the [[Retries.Budget]], whether through
 * retries due to failures or backup requests.
 *
 * @param maxExtraLoadTunable How much extra load, as a [[Tunable]] fraction, we are willing to send
 *                            to the server. Must be between 0.0 and 1.0. When this [[Tunable]] is
 *                            changed, it can take a few seconds for the new value to take effect.
 *                            Note that the max extra load is enforced by a [[RetryBudget]], which
 *                            is re-created on updates to the [[Tunable]] value; the existing
 *                            balance is *not* transferred to the new budget.
 *
 * @param sendInterrupts Whether or not to interrupt the original or backup request when a response
 *                       is returned and the result of the outstanding request is superseded. For
 *                       protocols without a control plane, where the connection is cut on
 *                       interrupts, this should be "false" to avoid connection churn.
 *
 * @note If `sendInterrupts` is set to false, and for clients that mask interrupts (e.g. the
 *       Finagle Memcached client), both the original request and backup will be counted in stats,
 *       so tail latency improvements as a result of this filter will not be reflected in the
 *       request latency stats.
 */
private[client] class BackupRequestFilter[Req, Rep](
    maxExtraLoadTunable: Tunable[Double],
    sendInterrupts: Boolean,
    responseClassifier: ResponseClassifier,
    newRetryBudget: (Double, () => Long) => RetryBudget,
    clientRetryBudget: RetryBudget,
    nowMs: () => Long,
    statsReceiver: StatsReceiver,
    timer: Timer,
    windowedPercentileHistogramFac: () => WindowedPercentileHistogram)
  extends SimpleFilter[Req, Rep] with Closable {
  import BackupRequestFilter._

  def this(
    maxExtraLoadTunable: Tunable[Double],
    sendInterrupts: Boolean,
    responseClassifier: ResponseClassifier,
    clientRetryBudget: RetryBudget,
    statsReceiver: StatsReceiver,
    timer: Timer
  ) = this(
    maxExtraLoadTunable,
    sendInterrupts,
    responseClassifier,
    newRetryBudget = BackupRequestFilter.newRetryBudget,
    clientRetryBudget = clientRetryBudget,
    Stopwatch.systemMillis,
    statsReceiver,
    timer,
    () => new WindowedPercentileHistogram(
      // Based on testing, a window of 30 seconds and 3 buckets tracked request
      // latency well and had no noticeable performance difference vs. a greater number of
      // buckets.
      numBuckets = 3,
      bucketSize = 10.seconds,
      timer))


  @volatile private[this] var backupRequestRetryBudget: RetryBudget =
    newRetryBudget(getAndValidateMaxExtraLoad(maxExtraLoadTunable), nowMs)

  private[this] def percentileFromMaxExtraLoad(maxExtraLoad: Double): Double =
    (1.0 - maxExtraLoad) * 100

  private[this] val windowedPercentile: WindowedPercentileHistogram =
    windowedPercentileHistogramFac()

  @volatile private[this] var sendBackupAfter: Int = 0

  // For testing
  private[client] def sendBackupAfterDuration: Duration =
    Duration.fromMilliseconds(sendBackupAfter)

  // schedule timer to refresh `sendBackupAfter`, and refresh `backupRequestRetryBudget` in response
  // to changes to the value of `maxExtraLoadTunable`,
  private[this] val refreshTimerTask: TimerTask = {
    @volatile var curMaxExtraLoad = getAndValidateMaxExtraLoad(maxExtraLoadTunable)
    @volatile var percentile = percentileFromMaxExtraLoad(curMaxExtraLoad)
    timer.schedule(RefreshPercentileInterval) {
      val newMaxExtraLoad = getAndValidateMaxExtraLoad(maxExtraLoadTunable)
      if (curMaxExtraLoad != newMaxExtraLoad) {
        curMaxExtraLoad = newMaxExtraLoad
        percentile = percentileFromMaxExtraLoad(curMaxExtraLoad)
        backupRequestRetryBudget = newRetryBudget(curMaxExtraLoad, nowMs)
      }
      sendBackupAfter = windowedPercentile.percentile(percentile)
      sendAfterStat.add(sendBackupAfter)
    }
  }

  private[this] val sendAfterStat = statsReceiver.stat("send_backup_after_ms")
  private[this] val backupsSent = statsReceiver.counter("backups_sent")

  // Indicates that the backup request returned first, regardless of whether it succeeded.
  private[this] val backupsWon = statsReceiver.counter("backups_won")

  private[this] val budgetExhausted = statsReceiver.counter("budget_exhausted")

  private[this] def isSuccess(reqRep: ReqRep): Boolean =
    responseClassifier.applyOrElse(reqRep, ResponseClassifier.Default) match {
      case ResponseClass.Successful(_) => true
      case ResponseClass.Failed(_) => false
    }

  // Record latency for successful responses, timeouts, and those we interrupted with
  // [[SupersededRequestFailure]]. We assume that other failures are "fast failures" and thus
  // would skew `sendBackupAfter` to be lower.
  private[this] def shouldRecord(response: Try[Rep]): Boolean = response match {
    case _ if isSuccess(ReqRep(response, response)) => true
    case Throw(_: IndividualRequestTimeoutException) => true
    case Throw(SupersededRequestFailure) => true
    case _ => false
  }

  private[this] def record(req: Req, f: Future[Rep]): Future[Rep] = {
    val start = nowMs()
    f.respond { response =>
      if (shouldRecord(response)) {
        val latency = (nowMs() - start).toInt
        windowedPercentile.add(latency)
      }
    }
  }

  // If withdrawing from `backupRequestRetryBudget` succeeds but withdrawing
  // from `clientRetryBudget` fails, we have withdrawn from `backupRequestRetryBudget` without
  // issuing a retry. However, since `backupRequestRetryBudget` is likely to be the limiting factor,
  // this should rarely happen and we tolerate it.
  private[this] def canIssueBackup(): Boolean =
    backupRequestRetryBudget.tryWithdraw() && clientRetryBudget.tryWithdraw()

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    backupRequestRetryBudget.deposit()
    val orig = record(req, service(req))
    val howLong = sendBackupAfter

    if (howLong == 0)
      return orig

    orig.within(
      timer,
      Duration.fromMilliseconds(howLong),
      OrigRequestTimeout
    ).transform {
      case Throw(OrigRequestTimeout) =>
        // If we've waited long enough to fire the backup normally, do so and
        // pass on the first successful result we get back.
        if (canIssueBackup()) {
          backupsSent.incr()
          val backup = record(req, service(req))
          orig.select(backup).transform { _ =>
            val winner = if (orig.isDefined) orig else backup
            val loser = if (winner eq orig) backup else orig
            winner.transform { response =>
              if (backup eq winner) backupsWon.incr()
              if (isSuccess(ReqRep(req, response))) {
                if (sendInterrupts) {
                  loser.raise(SupersededRequestFailure)
                }
                Future.const(response)
              } else {
                loser
              }
            }
          }
        } else {
          budgetExhausted.incr()
          orig
        }
      case _ =>
        // Return the original request when it completed first (regardless of success)
        orig
    }
  }

  def close(deadline: Time): Future[Unit] = {
    refreshTimerTask.cancel()
    windowedPercentile.close(deadline)
  }
}
