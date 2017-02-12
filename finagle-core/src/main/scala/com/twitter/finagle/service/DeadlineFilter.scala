package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Level
import com.twitter.util.{Future, Duration, Stopwatch, Time, TokenBucket}


/**
 * DeadlineFilter provides an admission control module that can be pushed onto the stack to
 * reject requests with expired deadlines (deadlines are set in the TimeoutFilter).
 * For servers, DeadlineFilter.module should be pushed onto the stack after the stats filters so
 * stats are recorded for the request, and before TimeoutFilter where a new Deadline is set.
 * For clients, DeadlineFilter.module should be after the stats filters; higher in the stack is
 * preferable so requests are rejected as early as possible. 
 *
 * @note Deadlines cross process boundaries and can span multiple nodes in a call graph.
 *       Even if a direct caller doesn't set a deadline, the server may still receive one and thus
 *       potentially fail the request. It is advised that all clients in the call graph be
 *       well prepared to handle NACKs before using this.
 */
object DeadlineFilter {

  private[this] val DefaultRejectPeriod = 10.seconds

  // In the case of large delays, don't want to reject too many requests
  private[this] val DefaultMaxRejectFraction = 0.2

  // Token Bucket is integer-based, so use a scale factor to facilitate
  // usage with the Double `maxRejectFraction`
  private[service] val RejectBucketScaleFactor = 1000.0

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.DeadlineFilter]] module.
   *
   * @param maxRejectFraction Maximum fraction of requests that can be
   *        rejected over `rejectPeriod`. Must be between 0.0 and 1.0.
   */
  case class Param(maxRejectFraction: Double) {
    require(maxRejectFraction >= 0.0 && maxRejectFraction <= 1.0,
      s"maxRejectFraction must be between 0.0 and 1.0: $maxRejectFraction")

    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param: Stack.Param[DeadlineFilter.Param] = Stack.Param(
      Param(DefaultMaxRejectFraction))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.DeadlineFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Stats, DeadlineFilter.Param, ServiceFactory[Req, Rep]] {
      val role = new Stack.Role("DeadlineFilter")
      val description = "Reject requests when their deadline has passed"

      def make(
        _stats: param.Stats,
        _param: DeadlineFilter.Param,
        next: ServiceFactory[Req, Rep]
      ) = {
        val Param(maxRejectFraction) = _param

        if (maxRejectFraction <= 0.0) next else {
          val param.Stats(statsReceiver) = _stats
          val scopedStatsReceiver = statsReceiver.scope("admission_control", "deadline")

          new ServiceFactoryProxy[Req, Rep](next) {

            private[this] val newDeadlineFilter: Service[Req, Rep] => Service[Req, Rep] = service =>
              new DeadlineFilter(
                DefaultRejectPeriod,
                maxRejectFraction,
                scopedStatsReceiver).andThen(service)

            override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
              // Create a DeadlineFilter per connection, so we don't share the state of the token
              // bucket for rejecting requests.
              next(conn).map(newDeadlineFilter)
          }
        }
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that rejects requests when their deadline
 * has passed.
 *
 * @param rejectPeriod No more than `maxRejectFraction` of requests will be
 *        discarded over the `rejectPeriod`. Must be `>= 1 second` and `<= 60 seconds`.
 * @param maxRejectFraction Maximum fraction of requests per-connection that can be
 *        rejected over `rejectPeriod`. Must be between 0.0 and 1.0.
 * @param statsReceiver for stats reporting, typically scoped to
 *        ".../admission_control/deadline/"
 * @param nowMillis current time in milliseconds
 * @see The [[https://twitter.github.io/finagle/guide/Servers.html#request-deadline user guide]]
 *      for more details.
 */
private[finagle] class DeadlineFilter[Req, Rep](
    rejectPeriod: Duration,
    maxRejectFraction: Double,
    statsReceiver: StatsReceiver,
    nowMillis: () => Long = Stopwatch.systemMillis)
  extends SimpleFilter[Req, Rep] {

  require(rejectPeriod.inSeconds >= 1 && rejectPeriod.inSeconds <= 60,
    s"rejectPeriod must be [1 second, 60 seconds]: $rejectPeriod")
  require(maxRejectFraction <= 1.0,
    s"maxRejectFraction must be between 0.0 and 1.0: $maxRejectFraction")

  private[this] val exceededStat = statsReceiver.counter("exceeded")
  private[this] val rejectedStat = statsReceiver.counter("rejected")
  private[this] val expiredTimeStat = statsReceiver.stat("expired_ms")

  private[this] val serviceDeposit =
    DeadlineFilter.RejectBucketScaleFactor.toInt
  private[this] val rejectWithdrawal =
    (DeadlineFilter.RejectBucketScaleFactor / maxRejectFraction).toInt

  private[this] val rejectBucket = TokenBucket.newLeakyBucket(
    rejectPeriod, 0, nowMillis)

  private[this] def newException(timestamp: Time, deadline: Time, elapsed: Duration, now: Time) =
    new Failure(
      s"exceeded request deadline of ${deadline - timestamp} " +
      s"by $elapsed. Deadline expired at $deadline and now it is $now.",
      flags = Failure.NonRetryable | Failure.Rejected, logLevel = Level.DEBUG)

  // The request is rejected if the set deadline has expired and there are at least
  // `rejectWithdrawal` tokens in `rejectBucket`. Otherwise, the request is
  // serviced and `serviceDeposit` tokens are added to `rejectBucket`.
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    Deadline.current match {
      case Some(Deadline(timestamp, deadline)) =>
        val now = Time.now
        val remaining = deadline - now

        if (deadline < now) {
          val exceeded = now - deadline
          exceededStat.incr()
          expiredTimeStat.add(exceeded.inMillis)

          // There are enough tokens to reject the request
          if (rejectBucket.tryGet(rejectWithdrawal)) {
            rejectedStat.incr()
            Future.exception(newException(timestamp, deadline, exceeded, now))
          } else {
            rejectBucket.put(serviceDeposit)
            service(request)
          }
        } else {
          rejectBucket.put(serviceDeposit)
          service(request)
        }
      case None =>
        rejectBucket.put(serviceDeposit)
        service(request)
    }
  }
}
