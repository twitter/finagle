package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle.loadbalancer.{Balancer, PeakEwma, Updating}
import com.twitter.util.{Activity, Duration}
import com.twitter.finagle.util.Rng
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{NoBrokersAvailableException, ServiceFactory}

/**
 * Like [[com.twitter.finagle.loadbalancer.p2c.P2CLeastLoaded]] but
 * using the Peak EWMA load metric.
 *
 * Peak EWMA is designed to converge quickly when encountering
 * slow endpoints. It is quick to react to latency spikes, recovering
 * only cautiously. Peak EWMA takes history into account, so that
 * slow behavior is penalized relative to the supplied decay time.
 *
 * @param endpoints An activity that updates with the set of node pairs
 * over which we distribute load.
 *
 * @param decayTime The window of latency observations.
 *
 * @param maxEffort the maximum amount of "effort" we're willing to
 * expend on a load balancing decision without reweighing.
 *
 * @param rng The PRNG used for flipping coins. Override for
 * deterministic tests.
 *
 * @param statsReceiver The stats receiver to which operational
 * statistics are reported.
 *
 * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
 * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
 * 10 (October 2001), 1094-1104.
 */
private[loadbalancer] class P2CPeakEwma[Req, Rep](
    protected val endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
    protected val decayTime: Duration,
    protected val maxEffort: Int,
    protected val rng: Rng,
    protected val statsReceiver: StatsReceiver,
    protected val emptyException: NoBrokersAvailableException)
  extends Balancer[Req, Rep]
  with PeakEwma[Req, Rep]
  with P2C[Req, Rep]
  with Updating[Req, Rep] {
  protected[this] val maxEffortExhausted = statsReceiver.counter("max_effort_exhausted")
}