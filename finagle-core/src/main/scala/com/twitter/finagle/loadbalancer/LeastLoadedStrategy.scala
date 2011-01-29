package com.twitter.finagle.loadbalancer

import com.twitter.conversions.time._

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.util.WeakMetadata
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
  private[this] val loadStat = WeakMetadata[ReadableCounter] {
    (new TimeWindowedStatsRepository(10, 1.seconds)).counter()
  }

  // TODO: account for recently introduced services.
  private[this] val leastLoadedOrdering = new Ordering[ServiceFactory[Req, Rep]] {
    def compare(a: ServiceFactory[Req, Rep], b: ServiceFactory[Req, Rep]) =
      loadStat(a).sum - loadStat(b).sum
  }

  def apply(pools: Seq[ServiceFactory[Req, Rep]]) = {
    val selected = pools.min(leastLoadedOrdering)
    loadStat(selected).incr()
    selected.make()
  }
}
