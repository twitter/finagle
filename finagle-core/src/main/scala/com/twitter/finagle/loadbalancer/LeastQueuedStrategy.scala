package com.twitter.finagle.loadbalancer

import util.Random
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
  private[this] val leastQueuedOrdering =
    Ordering.by { case (_, queueSize) => queueSize }: Ordering[(ServiceFactory[Req, Rep], Int)]

  private[this] def leastQueued(pools: Seq[ServiceFactory[Req, Rep]]) = {
    val snapshot = pools map { pool => (pool, queueStat(pool).get) }
    val shuffled = rng.shuffle(snapshot)
    val (selected, _) = shuffled.min(leastQueuedOrdering)
    selected
  }

  def apply(pools: Seq[ServiceFactory[Req, Rep]]) = {
    val pool = leastQueued(pools)
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
