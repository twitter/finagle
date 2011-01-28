package com.twitter.finagle.loadbalancer

import com.twitter.util.MapMaker

import com.twitter.finagle.Service

/**
 * A convenience class to keep per-service metadata
 */
class ServiceMetadata[T](default: => T) {
  private[this] val serviceToMetadata =
    MapMaker[Service[_, _], T] { config =>
      config.compute { key =>
        default
      }
      config.weakKeys
    }

  def apply(service: Service[_, _]) =
    serviceToMetadata(service)
}

object ServiceMetadata {
  def apply[T](default: => T) = new ServiceMetadata[T](default)
}

