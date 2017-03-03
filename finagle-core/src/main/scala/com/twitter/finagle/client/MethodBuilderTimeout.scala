package com.twitter.finagle.client

import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.util.{Duration, Future}
import scala.collection.mutable

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

  private[client] def totalFilter: Filter.TypeAgnostic = {
    val config = mb.config.timeout
    if (!config.total.isFinite) {
      if (config.stackHadTotalTimeout)
        DynamicTimeout.totalFilter(mb.params) // use their defaults
      else
        Filter.TypeAgnostic.Identity
    } else {
      new Filter.TypeAgnostic {
        def toFilter[Req1, Rep1]: Filter[Req1, Rep1, Req1, Rep1] = {
          val dyn = new SimpleFilter[Req1, Rep1] {
            def apply(req: Req1, service: Service[Req1, Rep1]): Future[Rep1] = {
              DynamicTimeout.letTotalTimeout(config.total) {
                service(req)
              }
            }
          }
          dyn.andThen(DynamicTimeout.totalFilter(mb.params).toFilter)
        }
      }
    }
  }

  /**
   * A filter that sets the proper state for per-request timeouts to do the
   * right thing down in the Finagle client stack.
   */
  private[client] def perRequestFilter: Filter.TypeAgnostic = {
    val config = mb.config.timeout
    if (!config.perRequest.isFinite) {
      Filter.TypeAgnostic.Identity
    } else {
      new Filter.TypeAgnostic {
        def toFilter[Req1, Rep1]: Filter[Req1, Rep1, Req1, Rep1] = {
          new SimpleFilter[Req1, Rep1] {
            def apply(req: Req1, service: Service[Req1, Rep1]): Future[Rep1] = {
              DynamicTimeout.letPerRequestTimeout(config.perRequest) {
                service(req)
              }
            }
          }
        }
      }
    }
  }

  private[client] def registryEntries: Iterable[(Seq[String], String)] = {
    val entries = new mutable.ListBuffer[(Seq[String], String)]()

    val perReq = mb.config.timeout.perRequest
    if (perReq.isFinite)
      entries += ((Seq("timeout", "per_request"), perReq.toString))
    val total = mb.config.timeout.total
    if (total.isFinite)
      entries += ((Seq("timeout", "total"), total.toString))

    entries
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
