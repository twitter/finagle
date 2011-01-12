package com.twitter.finagle.channel

import com.twitter.finagle.service.Service

// TODO: variables.
trait Broker extends Service[Any, Any] {
  def isAvailable: Boolean = true
}

trait WrappingBroker extends Broker {
  protected val underlying: Broker

  def apply(request: Any) = underlying(request)
  override def isAvailable = underlying.isAvailable
}
