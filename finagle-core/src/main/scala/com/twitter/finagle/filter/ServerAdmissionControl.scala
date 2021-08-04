package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.util.{Future, Time}

/**
 * Register and install admission control filters in the server Stack.
 *
 * Users can define their own admission control filters, which reject requests
 * when the server operates beyond its capacity. These rejections apply backpressure
 * and allow clients to retry requests on servers that may not be over capacity.
 * The filter implementation should define its own logic to determine over capacity.
 *
 * One or more admission control filters can be installed through the ``register`` method.
 * The filters are installed in a specific spot in the server Stack, but their internal
 * order does not matter. Admission control is enabled through
 * [[ServerAdmissionControl.Param]]. Each filter should provide its own mechanism
 * for enabling, disabling and configuration.
 *
 * Additionally, functions of [[ServerAdmissionControl.ServerParams]] => [[Filter]]
 * can also be registered, allowing for more fine-grained behavior.
 */
private[twitter] object ServerAdmissionControl {

  /**
   * Signal that the request cannot be retried by the client and thus the server
   * should attempt to handle it, if at all possible.
   */
  private[twitter] val NonRetryable: Contexts.local.Key[Unit] = Contexts.local.newKey[Unit]()

  /**
   * Passed to filter factories to allow behavioral adjustment on a per-service
   * basis rather than globally
   */
  case class ServerParams(protocol: String)

  val role = Stack.Role("Server Admission Controller")

  /**
   * The class is eligible for enabling admission control filters in the server Stack.
   *
   * @param serverAdmissionControlEnabled On/off switch for all admission controllers.
   *                                      When this is set to `false`, all requests
   *                                      bypass admission control.
   *
   * @see [[com.twitter.finagle.filter.ServerAdmissionControl]]
   */
  case class Param(serverAdmissionControlEnabled: Boolean)
  object Param {
    implicit val param = new Stack.Param[Param] {
      lazy val default = Param(true)
    }
  }

  /**
   * A collection of filter factories that will be used for admission control.
   *
   * Entries are keyed via name to prevent duplicate filters being inserted.
   */
  final case class Filters(filters: Map[String, ServerParams => TypeAgnostic])
  object Filters {
    implicit val param: Stack.Param[Filters] = Stack.Param(Filters(Map.empty))
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = {
    new Stack.Module4[Param, ProtocolLibrary, param.Stats, Filters, ServiceFactory[Req, Rep]] {
      val role = ServerAdmissionControl.role
      val description = "Proactively reject requests when the server operates beyond its capacity"

      def make(
        _enabled: ServerAdmissionControl.Param,
        protoLib: ProtocolLibrary,
        stats: param.Stats,
        acFilters: Filters,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val enabled = _enabled.serverAdmissionControlEnabled
        val protoString = protoLib.name
        val conf = ServerParams(protoString)
        val filters = acFilters.filters.values

        if (!enabled || filters.isEmpty) {
          next
        } else {
          // assume the order of filters doesn't matter
          val typeAgnosticFilters =
            filters.foldLeft(Filter.TypeAgnostic.Identity) {
              case (sum, mkFilter) =>
                mkFilter(conf).andThen(sum)
            }

          // Add our predicate filter so we don't reject requests that can't be retried
          val filter = new NonretryableFilter[Req, Rep](
            typeAgnosticFilters.toFilter,
            protoLib.name,
            stats.statsReceiver
          )

          new ServiceFactoryProxy[Req, Rep](filter.andThen(next)) {
            override def close(deadline: Time): Future[Unit] = {
              self.close(deadline)
            }
          }
        }
      }
    }
  }

  /**
   * It's the job of admission controllers to nack requests, but not all
   * requests are retryable (e.g. HTTP streams). If we know (by inspecting the
   * local context) that a request isn't safe to retry, then we bypass
   * admission control.
   */
  def bypassNonRetryable[Req, Rep](
    serverACFilter: Filter[Req, Rep, Req, Rep],
    protocolName: String,
    statsReceiver: StatsReceiver
  ): SimpleFilter[Req, Rep] =
    new NonretryableFilter(serverACFilter, protocolName, statsReceiver)

  private final class NonretryableFilter[Req, Rep](
    serverACFilter: Filter[Req, Rep, Req, Rep],
    protocolName: String,
    statsReceiver: StatsReceiver)
      extends SimpleFilter[Req, Rep] {

    private[this] val unRetryableCount =
      statsReceiver.counter(Verbosity.Debug, "admission_control", protocolName, "nonretryable")

    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
      // If the marker context element exists we presume the client can't retry the request
      if (Contexts.local.contains(NonRetryable)) {
        unRetryableCount.incr()
        // We clear the value since at this time there is little value for the service to know
        // whether the request is non-retryable. This could change in the future, especially if
        // people want this information for their own application level nacking logic.
        Contexts.local.letClear(NonRetryable) {
          service(request)
        }
      } else {
        serverACFilter(request, service)
      }
    }
  }

}
