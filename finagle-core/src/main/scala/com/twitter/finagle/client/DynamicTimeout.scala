package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.util.Duration
import com.twitter.util.tunable.Tunable

/**
 * Used for the creation of [[Stack]] modules that have dynamic timeouts.
 *
 * @see [[TimeoutFilter]]
 * @see [[LatencyCompensation]]
 */
object DynamicTimeout {

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
    defaultTunableTimeout: Tunable[Duration],
    defaultTimeout: Duration,
    latencyCompensation: Duration
  ): () => Duration = () => {
    val withoutCompensation = {
      val to = Contexts.local.getOrElse(timeoutKey, UseDefaultTimeoutFn)
      if (to eq UseDefaultTimeout) {
        defaultTunableTimeout() match {
          case Some(duration) => duration
          case None => defaultTimeout
        }
      } else to
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
    new Stack.ModuleParams[
      ServiceFactory[Req, Rep]
    ] {
      val role: Stack.Role = TimeoutFilter.role
      val description: String =
        "Apply a dynamic timeout-derived deadline to request"

      val parameters = Seq(
        implicitly[Stack.Param[TimeoutFilter.Param]],
        implicitly[Stack.Param[param.Timer]],
        implicitly[Stack.Param[LatencyCompensation.Compensation]],
        implicitly[Stack.Param[TimeoutFilter.PropagateDeadlines]],
        implicitly[Stack.Param[TimeoutFilter.PreferDeadlineOverTimeout]]
      )

      override def make(
        params: Stack.Params,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {

        val filter = new TimeoutFilter[Req, Rep](
          timeoutFn(
            PerRequestKey,
            params[TimeoutFilter.Param].tunableTimeout,
            TimeoutFilter.Param.Default, // tunableTimeout() should always produce a value,
            params[
              LatencyCompensation.Compensation].howlong // but we fall back on the default if not
          ),
          duration => new IndividualRequestTimeoutException(duration),
          params[param.Timer].timer,
          params[TimeoutFilter.PropagateDeadlines].enabled,
          params[TimeoutFilter.PreferDeadlineOverTimeout].enabled
        )
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
  private[client] def totalFilter(params: Stack.Params): Filter.TypeAgnostic = {
    val tunableTimeout = params[TimeoutFilter.TotalTimeout].tunableTimeout
    // tunableTimeout() should always produce a value, but we fall back on the default if not
    val defaultTimeout = TimeoutFilter.TotalTimeout.Default
    val compensation = params[LatencyCompensation.Compensation].howlong
    val timer = params[param.Timer].timer
    val timeoutFunc = timeoutFn(TotalKey, tunableTimeout, defaultTimeout, compensation)
    val exceptionFn = { d: Duration => new GlobalRequestTimeoutException(d) }
    val preferDeadlineOverTimeout = params[TimeoutFilter.PreferDeadlineOverTimeout].enabled
    val statsReceiver = params[Stats].statsReceiver
    TimeoutFilter.typeAgnostic(
      timeoutFunc,
      exceptionFn,
      timer,
      preferDeadlineOverTimeout,
      statsReceiver,
      TimeoutFilter.clientKey)
  }

}
