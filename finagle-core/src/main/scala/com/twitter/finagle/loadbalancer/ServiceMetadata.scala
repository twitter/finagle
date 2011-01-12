package com.twitter.finagle.loadbalancer

import com.twitter.util.MapMaker

import com.twitter.finagle.service.Service

/**
 * A convenience class to keep per-service metadata
 */
class ServiceMetadata[T](default: => T) {
  private[this] val serviceToMetadata =
    MapMaker[Service[_, _], T] { config => config.weakKeys }

  def apply(service: Service[_, _]) =
    serviceToMetadata.getOrElseUpdate(service, default)
}

object ServiceMetadata {
  def apply[T](default: => T) = new ServiceMetadata[T](default)
}

