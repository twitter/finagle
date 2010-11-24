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

// A filter is a service transform.
abstract class Filter[Req, Rep, Req1, Rep1]
  extends ((Req, Service[Req1, Rep]) => Future[Rep1])
{
  // def apply(request: Request, service: Service[Req1, Rep]): Future[Rep1]

  def andThen(next: Filter[Req1, Rep]): Filter[Req, Rep1] =
    new Filter[Req, Rep1] {
      def apply(request: Req, service: Service[Req1, Rep]) = {
        Filter.this.
      }
    }
}
