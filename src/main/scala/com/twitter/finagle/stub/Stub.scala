package com.twitter.finagle.stub

import com.twitter.util.Future

abstract class Stub[Req <: AnyRef, Rep <: AnyRef] {
  def apply(request: Req): Future[Rep] = call(request)
  def call(request: Req): Future[Rep]
}
