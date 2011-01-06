package com.twitter.finagle.channel

import java.util.concurrent.TimeUnit

import org.jboss.netty.util.HashedWheelTimer

import com.twitter.finagle.service.Service

// TODO: variables.
trait Broker extends Service[AnyRef, AnyRef] {
  def isAvailable: Boolean = true  
}

trait WrappingBroker extends Broker {
  val underlying: Broker

  def apply(request: AnyRef) = underlying(request)
  override def isAvailable = underlying.isAvailable
}

object Broker {
  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
}
