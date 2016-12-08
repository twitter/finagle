package com.twitter.finagle.client

import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.util.{Duration, Future}

/**
 * '''Experimental:''' This API is under construction.
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
   * @param howLong how long, from the initial request issuance,
   *                is the request given to complete.
   *                If it is not finite (e.g. `Duration.Top`),
   *                no method specific timeout will be applied.
   */
  def total(howLong: Duration): MethodBuilder[Req, Rep] = {
    val timeouts = mb.config.timeout.copy(total = howLong)
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

}

private[client] object MethodBuilderTimeout {

  /**
   * @param stackHadTotalTimeout indicates the stack originally had a total
   *                             timeout module. if `total` does not get
   *                             overridden by the module, it must still be added
   *                             back.
   * @param total this includes retries, connection setup, etc
   */
  case class Config(
      stackHadTotalTimeout: Boolean,
      total: Duration = Duration.Undefined)

}
