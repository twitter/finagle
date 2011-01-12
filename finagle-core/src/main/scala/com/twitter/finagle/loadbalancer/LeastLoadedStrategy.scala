package com.twitter.finagle.loadbalancer

import com.twitter.conversions.time._

import com.twitter.finagle.service.Service
import com.twitter.finagle.stats.{ReadableCounter, TimeWindowedStatsRepository}


/**
 * The "least loaded" strategy will dispatch the next request to the
 * host with the fewest dispatches within the (parameterized) time
 * window.
 */
class LeastLoadedStrategy[Req, Rep]
  extends LoadBalancerStrategy[Req, Rep]
{
  // TODO: threadsafety of stats?
  private[this] val loadStat = ServiceMetadata[ReadableCounter] {
    (new TimeWindowedStatsRepository(10, 1.seconds)).counter()
  }

  // TODO: account for recently introduced services.
  private[this] val leastLoadedOrdering = new Ordering[Service[Req, Rep]] {
    def compare(a: Service[Req, Rep], b: Service[Req, Rep]) =
      loadStat(a).sum - loadStat(b).sum
  }

  def dispatch(request: Req, services: Seq[Service[Req, Rep]]) =
    if (services.isEmpty) {
      None
    } else {
      val selected = services.min(leastLoadedOrdering)
      loadStat(selected).incr()
      Some((selected, selected(request)))
    }
}
