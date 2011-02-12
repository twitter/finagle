package com.twitter.finagle

import scala.util.Random

import com.twitter.util.Local

// beginTime, endTime, transcript, isTracing
case class RequestContext(var transactionID: Long)

object RequestContext {
  private[this] val rng = new Random
  private[this] val current = new Local[RequestContext]

  def update(ctx: RequestContext) {
    current() = ctx
  }

  def apply(): RequestContext = {
    if (!current().isDefined)
      current() = newContext()

    current().get
  }

  def newContext() = RequestContext(rng.nextLong())
}
