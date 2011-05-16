package com.twitter.finagle.loadbalancer

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.MapMaker
import com.twitter.finagle.stats.{Gauge, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{Service, ServiceFactory, ServiceFactoryProxy}

/**
 * The "least queued" strategy will produce weights inversely
 * proportional to the number of queued requests. Highly-queued
 * factories will have weights near zero.
 */
class LeastQueuedStrategy(statsReceiver: StatsReceiver)
  extends LoadBalancerStrategy
{
  def this() = this(NullStatsReceiver)

  private[this] val queueStat =
    MapMaker[ServiceFactory[_, _], (AtomicInteger, Gauge)] { config =>
      config.compute { factory =>
        val queueSize = new AtomicInteger(0)
        // We tag a gauge along with it to measure queue sizes.
        val gauge = statsReceiver.scope("queue_size").addGauge(factory.toString) {
          queueSize.get.toFloat
        }
        (queueSize, gauge)
      }
    }

  override def apply[Req, Rep](factories: Seq[ServiceFactory[Req, Rep]]) = {
    var totalLoad = 0

    val loadedFactories = factories map { factory =>
      val (qs, _) = queueStat(factory)
      val load = qs.get
      totalLoad += load
      (factory, load)
    }

    loadedFactories map { case (factory, load) =>
      val weight = if (totalLoad == 0) 1F else 1 - (load.toFloat / totalLoad)
      (annotate(factory), weight)
    }
  }

  private[this] def annotate[Req, Rep](underlying: ServiceFactory[Req, Rep]) =
    new ServiceFactoryProxy[Req, Rep](underlying) {
      override def make() = {
        val (qs, _) = queueStat(self)
        qs.incrementAndGet()

        self.make() map { service =>
          new Service[Req, Rep] {
            def apply(request: Req) = service(request)
            override def release() {
              service.release()
              qs.decrementAndGet()
            }
          }
        } onFailure { _ =>
          // The factory creation failed.
          qs.decrementAndGet()
        }
      }
  }
}
