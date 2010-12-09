package com.twitter.finagle.service

import com.twitter.util.Future

// Currently this interface doesn't support streaming responses.  This
// can be tackled in a number of ways:
//
//   - a similar continuation-future passing scheme
//   - create a Future[] subclass (eg. ContinuingFuture) that has the
//     logical flatMap implementation.

// Note: this is an abstract class (vs. a trait) to maintain java
// compatibility.
abstract class Service[-Req <: AnyRef, +Rep <: AnyRef] extends (Req => Future[Rep]) {
  def map[Req1 <: AnyRef](f: (Req1) => (Req)) = new Service[Req1, Rep] {
    def apply(req1: Req1) = Service.this.apply(f(req1))
  }
}

// A filter is a service transform [Req -> (Req1 -> Rep1) -> Rep].
abstract class Filter[-Req <: AnyRef, +Rep <: AnyRef, Req1 <: AnyRef, Rep1 <: AnyRef]
  extends ((Req, Service[Req1, Rep1]) => Future[Rep])
{
  def apply(request: Req, service: Service[Req1, Rep1]): Future[Rep]

  def andThen[Req2 <: AnyRef, Rep2 <: AnyRef](next: Filter[Req1, Rep1, Req2, Rep2]) =
    new Filter[Req, Rep, Req2, Rep2] {
      def apply(request: Req, service: Service[Req2, Rep2]) = {
        Filter.this.apply(request, new Service[Req1, Rep1] {
          def apply(request: Req1): Future[Rep1] = next(request, service)
        })
      }
    }

  def andThen(service: Service[Req1, Rep1]) = new Service[Req, Rep] {
    def apply(request: Req) = Filter.this.apply(request, service)
  }

  def andThenIf(condAndFilter: (Boolean, Filter[Req1, Rep1, Req1, Rep1])) =
    condAndFilter match {
      case (true, filter) => andThen(filter)
      case (false, _)     => this
    }

}
