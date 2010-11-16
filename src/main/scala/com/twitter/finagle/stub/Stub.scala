package com.twitter.finagle.stub

import com.twitter.util.Future

// Currently this interface doesn't support streaming responses.  This
// can be tackled in a number of ways:
//
//   - a similar continuation-future passing scheme
//   - create a Future[] subclass (eg. ContinuingFuture) that has the
//     logical flatMap implementation.

abstract class Stub[Req <: AnyRef, Rep <: AnyRef] {
  def apply(request: Req): Future[Rep] = call(request)
  def call(request: Req): Future[Rep]
}
