package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Duration, Future, Stopwatch, Time, TokenBucket}

object DeadlineFilter {

  val role = new Stack.Role("DeadlineAdmissionControl")

  private[this] val DefaultTolerance = 170.milliseconds // max empirically measured clock skew
  private[this] val DefaultRejectPeriod = 10.seconds

  // In the case of large delays, don't want to reject too many requests
  private[this] val DefaultMaxRejectPercentage = 0.2

  // Token Bucket is integer-based, so use a scale factor to facilitate
  // usage with the Double `maxRejectPercentage`
  private[service] val RejectBucketScaleFactor = 1000.0

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.DeadlineFilter]] module.
   *
   * @param tolerance The max elapsed time since a request's deadline when it
   * will be considered for rejection. Used to account for unreasonable clock
   * skew.
   *
   * @param maxRejectPercentage Maximum percentage of requests that can be
   * rejected over `rejectPeriod`. Must be between 0.0 and 1.0.
   */
  case class Param(
      tolerance: Duration,
      maxRejectPercentage: Double)
  {
    require(tolerance >= Duration.Zero,
      "tolerance must be greater than zero")

    require(maxRejectPercentage >= 0.0 && maxRejectPercentage <= 1.0,
      s"maxRejectPercentage must be between 0.0 and 1.0: $maxRejectPercentage")

    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(
      Param(DefaultTolerance, DefaultMaxRejectPercentage))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.DeadlineFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Stats, DeadlineFilter.Param, ServiceFactory[Req, Rep]] {
      val role = DeadlineFilter.role
      val description = "Reject requests when their deadline has passed"

      def make(
        _stats: param.Stats,
        _param: DeadlineFilter.Param,
        next: ServiceFactory[Req, Rep]
      ) = {
        val Param(tolerance, maxRejectPercentage) = _param
        val param.Stats(statsReceiver) = _stats

        if (maxRejectPercentage <= 0.0) next
        else
          new DeadlineFilter(
            tolerance,
            DefaultRejectPeriod,
            maxRejectPercentage,
            statsReceiver.scope("admission_control", "deadline")).andThen(next)
      }
  }
}

/**
 * A [[com.twitter.finagle.Filter]] that rejects requests when their deadline
 * has passed.
 *
 * @param tolerance The max elapsed time since a request's deadline when it will
 * be considered for rejection. Used to account for unreasonable clock skew.
 *
 * @param rejectPeriod No more than `maxRejectPercentage` of requests will be
 * discarded over the `rejectPeriod`. Must be `>= 1 second` and `<= 60 seconds`.
 *
 * @param maxRejectPercentage Maximum percentage of requests that can be
 * rejected over `rejectPeriod`. Must be between 0.0 and 1.0.
 *
 * @param statsReceiver for stats reporting, typically scoped to
 * ".../admission_control/deadline/"
 *
 * @param nowMillis current time in milliseconds
 *
 * @see The [[https://twitter.github.io/finagle/guide/Servers.html#request-deadline user guide]]
 *      for more details.
 */
private[finagle] class DeadlineFilter[Req, Rep](
    tolerance: Duration,
    rejectPeriod: Duration,
    maxRejectPercentage: Double,
    statsReceiver: StatsReceiver,
    nowMillis: () => Long = Stopwatch.systemMillis)
  extends SimpleFilter[Req, Rep] {

  require(tolerance >= Duration.Zero,
    "tolerance must be greater than zero")
  require(rejectPeriod.inSeconds >= 1 && rejectPeriod.inSeconds <= 60,
    s"rejectPeriod must be [1 second, 60 seconds]: $rejectPeriod")
  require(maxRejectPercentage <= 1.0,
    s"maxRejectPercentage must be between 0.0 and 1.0: $maxRejectPercentage")

  private[this] val exceededStat = statsReceiver.counter("exceeded")
  private[this] val beyondToleranceStat =
    statsReceiver.counter("exceeded_beyond_tolerance")
  private[this] val rejectedStat = statsReceiver.counter("rejected")
  private[this] val transitTimeStat = statsReceiver.stat("transit_latency_ms")
  private[this] val budgetTimeStat = statsReceiver.stat("deadline_budget_ms")

  private[this] val serviceDeposit =
    DeadlineFilter.RejectBucketScaleFactor.toInt
  private[this] val rejectWithdrawal =
    (DeadlineFilter.RejectBucketScaleFactor / maxRejectPercentage).toInt

  private[this] val rejectBucket = TokenBucket.newLeakyBucket(
    rejectPeriod, 0, nowMillis)

  private[this] def deadlineExceeded(
    deadline: Deadline,
    elapsed: Duration,
    now: Time
  ) = s"exceeded request deadline of ${deadline.deadline - deadline.timestamp} " +
    s"by $elapsed. Deadline expired at ${deadline.deadline} and now it is $now."

  // The request is rejected if the set deadline has expired, the elapsed time
  // since expiry is less than `tolerance`, and there are at least
  // `rejectWithdrawal` tokens in `rejectBucket`. Otherwise, the request is
  // serviced and `serviceDeposit` tokens are added to `rejectBucket`.
  //
  // Note: While in the testing stages, requests are never rejected, but
  // the admission_control/deadline/rejected stat is incremented.
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    Deadline.current match {
      case Some(deadline) =>
        val now = Time.now
        val remaining = deadline.deadline - now

        transitTimeStat.add((now - deadline.timestamp).max(Duration.Zero).inMilliseconds)
        budgetTimeStat.add(remaining.max(Duration.Zero).inMilliseconds)

        // Exceeded the deadline within tolerance, and there are enough
        // tokens to reject the request
        if (remaining < Duration.Zero
            && -remaining <= tolerance
            && rejectBucket.tryGet(rejectWithdrawal)) {
          exceededStat.incr()
          rejectedStat.incr()
          service(request)  // When turned on this will be Future.exception
        } else {
          if (-remaining > tolerance) beyondToleranceStat.incr()
          else if (remaining < Duration.Zero) exceededStat.incr()

          rejectBucket.put(serviceDeposit)
          service(request)
        }
      case None =>
        rejectBucket.put(serviceDeposit)
        service(request)
    }
}
