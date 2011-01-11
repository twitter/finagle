package com.twitter.finagle.channel

import com.twitter.finagle.service.Service

// TODO: variables.
trait Broker extends Service[AnyRef, AnyRef] {
  def isAvailable: Boolean = true
}

trait WrappingBroker extends Broker {
  protected val underlying: Broker

  def apply(request: AnyRef) = underlying(request)
  override def isAvailable = underlying.isAvailable
}