package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.util.Duration

/**
 * Used for the creation of [[Stack]] modules that have dynamic timeouts.
 *
 * @see [[TimeoutFilter]]
 * @see [[LatencyCompensation]]
 */
private[finagle] object DynamicTimeout {

  private[this] val PerRequestKey = new Contexts.local.Key[Duration]()
  private[this] val TotalKey = new Contexts.local.Key[Duration]()

  /**
   * A sentinel used to indicate that the default timeout should be used.
   *
   * Note that this should be compared using reference equality.
   */
  private[this] val UseDefaultTimeout = Duration.fromNanoseconds(-12345L)
  private[this] val UseDefaultTimeoutFn: () => Duration = () => UseDefaultTimeout

  /**
   * Sets a per-request timeout, scoped to `f`. It only works in conjunction
   * with a client using the [[perRequestModule]] installed in its stack.
   *
   * This applies to each attempt such that if there are retry requests,
   * each of them will have the same timeout.
   *
   * @param timeout no timeout will be applied if `timeout` is less than or
   *                equal to zero or if it is not finite.
   *
   * @see [[TimeoutFilter]]
   * @see This is similar in concept to
   *      [[com.twitter.finagle.builder.ClientBuilder.requestTimeout]], but allows
   *      for the timeouts to be dynamic and changed at runtime.
   */
  def letPerRequestTimeout[T](timeout: Duration)(f: => T): T =
    Contexts.local.let(PerRequestKey, timeout) { f }

  /**
   * Sets a total request timeout, scoped to `f`. It only works in conjunction
   * with a client using [[totalFilter]].
   *
   * This applies to the total request time, including retries.
   *
   * @param timeout no timeout will be applied if `timeout` is less than or
   *                equal to zero or if it is not finite.
   *
   * @see [[TimeoutFilter]]
   * @see This is similar in concept to
   *      [[com.twitter.finagle.builder.ClientBuilder.timeout]], but allows
   *      for the timeouts to be dynamic and changed at runtime.
   */
  def letTotalTimeout[T](timeout: Duration)(f: => T): T =
    Contexts.local.let(TotalKey, timeout) { f }

  private[this] def timeoutFn(
    timeoutKey: Contexts.local.Key[Duration],
    defaultTimeout: Duration,
    latencyCompensation: Duration
  ): () => Duration = () => {
    val withoutCompensation = {
      val to = Contexts.local.getOrElse(timeoutKey, UseDefaultTimeoutFn)
      if (to eq UseDefaultTimeout) defaultTimeout
      else to
    }
    if (latencyCompensation.isFinite) withoutCompensation + latencyCompensation
    else withoutCompensation
  }

  /**
   * A client module that produces a [[TimeoutFilter]] for the stack,
   * which allows for dynamic per-request timeouts (e.g. retry requests).
   *
   * This has a similar purpose to using
   * [[com.twitter.finagle.param.CommonParams.withRequestTimeout]], while
   * allowing the timeout to be dynamic.
   *
   * Note that any [[LatencyCompensation]] is added to timeouts.
   *
   * @see [[totalFilter]] for a total timeout including all retries.
   * @see [[TimeoutFilter]]
   * @see [[LatencyCompensation]]
   */
  def perRequestModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[
      TimeoutFilter.Param,
      param.Timer,
      LatencyCompensation.Compensation,
      ServiceFactory[Req, Rep]]
    {
      val role: Stack.Role = TimeoutFilter.role
      val description: String =
        "Apply a dynamic timeout-derived deadline to request"

      def make(
        defaultTimeout: TimeoutFilter.Param,
        timer: param.Timer,
        compensation: LatencyCompensation.Compensation,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val filter = new TimeoutFilter[Req, Rep](
          timeoutFn(PerRequestKey, defaultTimeout.timeout, compensation.howlong),
          duration => new IndividualRequestTimeoutException(duration),
          timer.timer)
        filter.andThen(next)
      }
    }

  /**
   * Produces a [[Filter.TypeAgnostic]] which allows for dynamic total timeouts
   * from a set of [[Stack.Params]].
   * These timeouts should encompass the time included in retry requests.
   *
   * This has a similar purpose to using a
   * [[com.twitter.finagle.builder.ClientBuilder.timeout]], while
   * allowing the timeout to be dynamic.
   *
   * Note that any [[LatencyCompensation]] is added to timeouts.
   *
   * @see [[perRequestModule]] for a per-request timeout.
   * @see [[TimeoutFilter]]
   * @see [[LatencyCompensation]]
   */
  private[client] def totalFilter(
    params: Stack.Params
  ): Filter.TypeAgnostic = {
    val defaultTimeout = params[TimeoutFilter.TotalTimeout].timeout
    val compensation = params[LatencyCompensation.Compensation].howlong
    val timer = params[param.Timer].timer
    val timeoutFunc = timeoutFn(TotalKey, defaultTimeout, compensation)
    val exceptionFn = { d: Duration => new GlobalRequestTimeoutException(d) }
    TimeoutFilter.typeAgnostic(
      timeoutFunc,
      exceptionFn,
      timer)
  }

}
