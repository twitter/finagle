package com.twitter.finagle.loadbalancer

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.util.WeakMetadata

/**
 * The "least queued" strategy will produce weights inversely
 * proportional to the number of queued requests. Highly-queued
 * factories will have weights near zero.
 */
class LeastQueuedStrategy()
  extends LoadBalancerStrategy
{
  private[this] val queueStat = WeakMetadata[AtomicInteger] { new AtomicInteger(0) }

  override def apply[Req, Rep](factories: Seq[ServiceFactory[Req, Rep]]) = {
    var totalLoad = 0

    val loadedFactories = factories map { factory =>
      val load = queueStat(factory).get
      totalLoad += load
      (factory, load)
    }

    loadedFactories map { case (factory, load) =>
      val weight = if (totalLoad == 0) 1F else 1 - (load.toFloat / totalLoad)
      (annotate(factory), weight)
    }
  }

  private[this] def annotate[Req, Rep](underlying: ServiceFactory[Req, Rep]) =
    new ServiceFactory[Req, Rep] {
      override def make() = {
        val qs = queueStat(underlying)
        qs.incrementAndGet()

        underlying.make() map { service =>
          new Service[Req, Rep] {
            def apply(request: Req) = service(request)
            override def release() {
              service.release()
              qs.decrementAndGet()
            }
          }
        }
      }
    override def close() = underlying.close()
    override def isAvailable = underlying.isAvailable
  }
}
