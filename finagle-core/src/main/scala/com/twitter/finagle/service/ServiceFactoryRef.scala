package com.twitter.finagle.service

import com.twitter.finagle.{ServiceFactoryProxy, ServiceFactory}
import com.twitter.util.Updatable

/**
 * An updatable service factory proxy.
 *
 * @param init The starting factory to proxy to
 */
private[finagle] class ServiceFactoryRef[Req, Rep](
  init: ServiceFactory[Req, Rep]
) extends ServiceFactoryProxy[Req, Rep](init) with Updatable[ServiceFactory[Req, Rep]] {
  @volatile private[this] var cur: ServiceFactory[Req, Rep] = init

  def update(newFactory: ServiceFactory[Req, Rep]) {
    cur = newFactory
  }

  override def self: ServiceFactory[Req, Rep] = cur
}
