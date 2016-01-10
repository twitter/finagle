package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.Filter.TypeAgnostic
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.collection.JavaConverters._

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
 */
private[twitter] object ServerAdmissionControl {
  // a map of admission control filters, key by name
  private[this] val acs: ConcurrentMap[String, TypeAgnostic] = new ConcurrentHashMap()

  val role = new Stack.Role("Server Admission Controller")

  /**
   * A class eligible for enabling admission control filters in the server Stack.
   *
   * @see [[com.twitter.finagle.filter.ServerAdmissionControl]]
   */
  case class Param(enabled: Boolean)
  object Param {
    implicit val param = new Stack.Param[Param] {
      lazy val default = Param(true)
    }
  }

  /**
   * Add a filter to the list of admission control filters. If a controller
   * with the same name already exists in the map, it's a no-op. It must
   * be called before the server construction to take effect.
   */
  def register(name: String, filter: TypeAgnostic): Unit =
    acs.putIfAbsent(name, filter)

  /**
   * Add multiple filters to the list of admission control filters. If a controller
   * with the same name already exists in the map, it's a no-op. It must
   * be called before the server construction to take effect.
   */
  def register(pairs: (String, TypeAgnostic)*): Unit =
    pairs.foreach { case (name, filter) =>
      acs.putIfAbsent(name, filter)
    }


  /**
   * Remove a filter from the list of admission control filters. If the map
   * does not contain a controller with the name, it's a no-op. It must
   * be called before the server construction to take effect.
   */
  def unregister(name: String): Unit = acs.remove(name)

  /**
   * Clear all filters from the list of admission control filters.
   */
  def unregisterAll(): Unit = acs.clear()

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = {
    new Stack.Module1[Param, ServiceFactory[Req, Rep]] {
      val role = ServerAdmissionControl.role
      val description = "Proactively reject requests when the server operates beyond its capacity"
      def make(
        _enabled: Param,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val Param(enabled) = _enabled

        if (!enabled || acs.isEmpty) {
          next
        } else {
          // assume the order of filters doesn't matter
          val typeAgnosticFilters =
            acs.values.asScala.foldLeft(Filter.TypeAgnostic.Identity){ case (sum, f) =>
              f.andThen(sum)
            }
          typeAgnosticFilters.toFilter.andThen(next)
        }
      }
    }
  }
}