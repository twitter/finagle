package com.twitter.finagle.loadbalancer

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.finagle.Service

/**
 * The "least queued" strategy will dispatch the next request to the
 * service with the fewest number of outstanding requests.
 */
class LeastQueuedStrategy[Req, Rep]
  extends LoadBalancerStrategy[Req, Rep]
{
  private[this] val queueStat = ServiceMetadata[AtomicInteger] { new AtomicInteger(0) }
  private[this] val leastQueuedOrdering =
    Ordering.by { case (_, queueSize) => queueSize }: Ordering[(Service[Req, Rep], Int)]

  def select(services: Seq[Service[Req, Rep]]) = {
    val snapshot = services map { service => (service, queueStat(service).get)  }
    val (selected, _) = snapshot.min(leastQueuedOrdering)
    selected
  }

  def dispatch(request: Req, services: Seq[Service[Req, Rep]]) = {
    if (services.isEmpty) {
      None
    } else {
      val service = select(services)
      val qs = queueStat(service)

      // Dispatch the request, and account for it.
      val replyFuture = service(request)

      qs.incrementAndGet()
      replyFuture respond { _ => qs.decrementAndGet() }

      Some((service, replyFuture))
    }
  }
}
