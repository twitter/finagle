package com.twitter.finagle.client

import com.twitter.finagle.Filter
import com.twitter.finagle.Service
import com.twitter.finagle.SimpleFilter
import com.twitter.util.tunable.Tunable
import com.twitter.util.Duration
import com.twitter.util.Future
import scala.collection.mutable

/**
 * @see [[BaseMethodBuilder]]
 */
private[finagle] class MethodBuilderTimeout[Req, Rep] private[client] (
  mb: MethodBuilder[Req, Rep]) {

  /**
   * @see [[BaseMethodBuilder.withTimeoutTotal(Duration)]]
   */
  def total(howLong: Duration): MethodBuilder[Req, Rep] = {
    val timeouts = mb.config.timeout.copy(total = mb.config.timeout.total.copy(duration = howLong))
    mb.withConfig(mb.config.copy(timeout = timeouts))
  }

  /**
   * @see [[BaseMethodBuilder.withTimeoutTotal(Tunable[Duration])]]
   */
  def total(howLong: Tunable[Duration]): MethodBuilder[Req, Rep] = {
    val timeouts = mb.config.timeout.copy(total = mb.config.timeout.total.copy(tunable = howLong))
    mb.withConfig(mb.config.copy(timeout = timeouts))
  }

  /**
   * @see [[BaseMethodBuilder.withTimeoutPerRequest(Duration)]]
   */
  def perRequest(howLong: Duration): MethodBuilder[Req, Rep] = {
    val timeouts =
      mb.config.timeout.copy(perRequest = mb.config.timeout.perRequest.copy(duration = howLong))
    mb.withConfig(mb.config.copy(timeout = timeouts))
  }

  /**
   * @see [[BaseMethodBuilder.withTimeoutPerRequest(Tunable[Duration])]]
   */
  def perRequest(howLong: Tunable[Duration]): MethodBuilder[Req, Rep] = {
    val timeouts =
      mb.config.timeout.copy(perRequest = mb.config.timeout.perRequest.copy(tunable = howLong))
    mb.withConfig(mb.config.copy(timeout = timeouts))
  }

  private[client] def totalFilter: Filter.TypeAgnostic = {
    val config = mb.config.timeout
    if (!config.total.isFinite &&
      !config.total.isTunable &&
      !config.stackTotalTimeoutDefined) {
      Filter.TypeAgnostic.Identity
    } else {
      new Filter.TypeAgnostic {
        def toFilter[Req1, Rep1]: Filter[Req1, Rep1, Req1, Rep1] = {
          val dyn = new SimpleFilter[Req1, Rep1] {
            def apply(req: Req1, service: Service[Req1, Rep1]): Future[Rep1] = {
              DynamicTimeout.letTotalTimeout(config.total.toDuration) {
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
    new Filter.TypeAgnostic {
      def toFilter[Req1, Rep1]: Filter[Req1, Rep1, Req1, Rep1] = {
        new SimpleFilter[Req1, Rep1] {
          def apply(req: Req1, service: Service[Req1, Rep1]): Future[Rep1] = {
            DynamicTimeout.letPerRequestTimeout(config.perRequest.toDuration) {
              service(req)
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
   * @param stackTotalTimeoutDefined indicates the stack originally had a total
   *                             timeout module. if `total` does not get
   *                             overridden by the module, it must still be added
   *                             back.
   * @param total this includes retries, connection setup, etc
   *
   * @param perRequest how long a '''single''' request is given to complete.
   */
  case class Config(
    stackTotalTimeoutDefined: Boolean,
    total: TunableDuration = TunableDuration("total"),
    perRequest: TunableDuration = TunableDuration("perRequest"))

  case class TunableDuration(
    id: String,
    duration: Duration = Duration.Undefined,
    tunable: Tunable[Duration] = Tunable.none) {
    def isFinite: Boolean = duration.isFinite
    def isTunable: Boolean = tunable != Tunable.none

    def toDuration: Duration = tunable() match {
      case Some(d) => d
      case None => duration
    }
  }
}
