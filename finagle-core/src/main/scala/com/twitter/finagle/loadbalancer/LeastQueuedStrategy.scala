package com.twitter.finagle.loadbalancer

import util.Random
import collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.util.WeakMetadata

/**
 * The "least queued" strategy will dispatch the next request to the
 * service with the fewest number of outstanding requests.
 */
class LeastQueuedStrategy[Req, Rep]
  extends LoadBalancerStrategy[Req, Rep]
{
  private[this] val rng = new Random
  private[this] val queueStat = WeakMetadata[AtomicInteger] { new AtomicInteger(0) }

  private[this] def leastQueued(factories: Seq[ServiceFactory[Req, Rep]]) = {
    var minLoad = Int.MaxValue
    val mins = new ArrayBuffer[ServiceFactory[Req, Rep]]

    factories foreach { factory =>
      val load = queueStat(factory).get
      if (load < minLoad) {
        mins.clear()
        mins += factory
        minLoad = load
      } else if (load == minLoad) {
        mins += factory
      }
    }

    if (mins.size == 1)
      mins(0)
    else 
      mins(rng.nextInt(mins.size))
  }

  def apply(pools: Seq[ServiceFactory[Req, Rep]]) = {
    val pool = if (pools.size == 1) pools.head else leastQueued(pools)
    val qs = queueStat(pool)
    qs.incrementAndGet()

    pool.make() map { service =>
      new Service[Req, Rep] {
        def apply(request: Req) = service(request)
        override def release() {
          service.release()
          qs.decrementAndGet()
        }
      }
    }
  }
}
