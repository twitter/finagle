package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.util.Fiber
import com.twitter.util.Future

private[finagle] object FiberForkFilter {

  val Description: String = "Fork requests into a new fiber"
  val Role: Stack.Role = Stack.Role("FiberForkFilter")

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      def role: Stack.Role = Role
      def description: String = Description

      private[this] val filter = new SimpleFilter[Req, Rep] {
        def apply(
          request: Req,
          service: Service[Req, Rep]
        ): Future[Rep] = {
          Fiber.let(Fiber.newCachedSchedulerFiber()) {
            service(request)
          }
        }
      }

      def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        filter.andThen(next)
      }
    }
}
