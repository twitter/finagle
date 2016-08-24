package com.twitter.finagle.filter

import com.twitter.finagle.context.Contexts
import com.twitter.finagle._
import com.twitter.util.Future

/**
 * ClearContextValueFilter clears the configured Context key in the request's
 * Context.
 */
private[finagle] object ClearContextValueFilter {
  val role = Stack.Role("ClearContextValue")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.Filter]] that clears the
   * broadcast Context value for a given key.
   */
  def module[Req, Rep](contextKey: Contexts.broadcast.Key[_]): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role = ClearContextValueFilter.role
      val description = s"Clear Context value for key: ${contextKey.id}"
      def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val filter = new SimpleFilter[Req, Rep] {
          def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
            Contexts.broadcast.letClear(contextKey) {
              service(request)
            }
          }
        }
        filter.andThen(next)
      }
    }
}
