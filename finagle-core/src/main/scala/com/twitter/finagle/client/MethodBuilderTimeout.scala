package com.twitter.finagle.client

import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.util.{Duration, Future}

/**
 * '''Experimental:''' This API is under construction.
 *
 * Defaults to having no timeouts set.
 *
 * @example To set a per-request timeout of 50 milliseconds and a total
 *          timeout of 100 milliseconds:
 *          {{{
 *          import com.twitter.conversions.time._
 *          import com.twitter.finagle.client.MethodBuilder
 *
 *          val builder: MethodBuilder[Int, Int] = ???
 *          builder
 *            .withTimeout.perRequest(50.milliseconds)
 *            .withTimeout.total(100.milliseconds)
 *          }}}
 *
 * @see [[MethodBuilder.withTimeout]]
 */
private[finagle] class MethodBuilderTimeout[Req, Rep] private[client] (
    mb: MethodBuilder[Req, Rep]) {

  /**
   * Set a total timeout, including time spent on retries.
   *
   * If the request does not complete in this time, the response
   * will be satisfied with a [[com.twitter.finagle.GlobalRequestTimeoutException]].
   *
   * @example
   * For example, a total timeout of 200 milliseconds:
   * {{{
   * import com.twitter.conversions.time._
   * import com.twitter.finagle.client.MethodBuilder
   *
   * val builder: MethodBuilder[Int, Int] = ???
   * builder.withTimeout.total(200.milliseconds)
   * }}}
   * @param howLong how long, from the initial request issuance,
   *                is the request given to complete.
   *                If it is not finite (e.g. `Duration.Top`),
   *                no method specific timeout will be applied.
   *
   * @see [[perRequest(Duration)]]
   */
  def total(howLong: Duration): MethodBuilder[Req, Rep] = {
    val timeouts = mb.config.timeout.copy(total = howLong)
    mb.withConfig(mb.config.copy(timeout = timeouts))
  }

  /**
   * How long a '''single''' request is given to complete.
   *
   * If there are [[MethodBuilderRetry retries]], each attempt is given up to
   * this amount of time.
   *
   * If a request does not complete within this time, the response
   * will be satisfied with a [[com.twitter.finagle.IndividualRequestTimeoutException]].
   *
   * @example
   * For example, a per-request timeout of 50 milliseconds:
   * {{{
   * import com.twitter.conversions.time._
   * import com.twitter.finagle.client.MethodBuilder
   *
   * val builder: MethodBuilder[Int, Int] = ???
   * builder.withTimeout.perRequest(50.milliseconds)
   * }}}
   *
   * @param howLong how long, from the initial request issuance,
   *                an individual attempt given to complete.
   *                If it is not finite (e.g. `Duration.Top`),
   *                no method specific timeout will be applied.
   *
   * @see [[total(Duration)]]
   */
  def perRequest(howLong: Duration): MethodBuilder[Req, Rep] = {
    val timeouts = mb.config.timeout.copy(perRequest = howLong)
    mb.withConfig(mb.config.copy(timeout = timeouts))
  }

  private[client] def totalFilter: Filter[Req, Rep, Req, Rep] = {
    val config = mb.config.timeout
    if (!config.total.isFinite) {
      if (config.stackHadTotalTimeout)
        DynamicTimeout.totalFilter[Req, Rep](mb.params) // use their defaults
      else
        Filter.identity[Req, Rep]
    } else {
      val dyn = new SimpleFilter[Req, Rep] {
        def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
          DynamicTimeout.letTotalTimeout(config.total) {
            service(req)
          }
        }
      }
      dyn.andThen(DynamicTimeout.totalFilter[Req, Rep](mb.params))
    }
  }

  /**
   * A filter that sets the proper state for per-request timeouts to do the
   * right thing down in the Finagle client stack.
   */
  private[client] def perRequestFilter: Filter[Req, Rep, Req, Rep] = {
    val config = mb.config.timeout
    if (!config.perRequest.isFinite) {
      Filter.identity[Req, Rep]
    } else {
      new SimpleFilter[Req, Rep] {
        def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
          DynamicTimeout.letPerRequestTimeout(config.perRequest) {
            service(req)
          }
        }
      }
    }
  }

}

private[client] object MethodBuilderTimeout {

  /**
   * @param stackHadTotalTimeout indicates the stack originally had a total
   *                             timeout module. if `total` does not get
   *                             overridden by the module, it must still be added
   *                             back.
   * @param total this includes retries, connection setup, etc
   *
   * @param perRequest how long a '''single''' request is given to complete.
   */
  case class Config(
      stackHadTotalTimeout: Boolean,
      total: Duration = Duration.Undefined,
      perRequest: Duration = Duration.Undefined)

}
