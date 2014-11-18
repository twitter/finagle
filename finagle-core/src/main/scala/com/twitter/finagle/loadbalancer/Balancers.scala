package com.twitter.finagle.loadbalancer

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.{Rng, DefaultTimer}
import com.twitter.finagle.{ServiceFactory, NoBrokersAvailableException}
import com.twitter.util.{Activity, Duration, Timer}
import scala.util.Random

/**
 * Constructors for various load balancers.
 */
object Balancers {
  /**
   * An O(1), concurrent, weighted least-loaded fair load balancer.
   * This uses the ideas behind "power of 2 choices" [1] combined with
   * O(1) biased coin flipping through the aliasing method, described
   * in [[com.twitter.finagle.util.Drv Drv]].
   *
   * @param underlying An activity that updates with the set of
   * (node, weight) pairs over which we distribute load.
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
  def newP2C[Req, Rep](
    activity: Activity[Traversable[(ServiceFactory[Req, Rep], Double)]],
    maxEffort: Int = 5,
    rng: Rng = Rng.threadLocal,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    emptyException: NoBrokersAvailableException = new NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] = new P2CBalancer(
    activity,  maxEffort, rng, statsReceiver, emptyException)

  /**
   * Like [[newP2C]] but using the Peak EWMA load metric.
   *
   * Peak EWMA is designed to converge quickly when encountering
   * slow endpoints. It is quick to react to latency spikes, recovering
   * only cautiously. Peak EWMA takes history into account, so that 
   * slow behavior is penalized relative to the supplied decay time.
   *
   * @param underlying An activity that updates with the set of
   * (node, weight) pairs over which we distribute load.
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

  def newP2CPeakEwma[Req, Rep](
    activity: Activity[Traversable[(ServiceFactory[Req, Rep], Double)]],
    decayTime: Duration,
    maxEffort: Int = 5,
    rng: Rng = Rng.threadLocal,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    emptyException: NoBrokersAvailableException = new NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] = new P2CBalancerPeakEwma(
    activity, decayTime, maxEffort, rng, 
    statsReceiver, emptyException)

  /**
   * An efficient strictly least-loaded balancer that maintains
   * an internal heap.
   */
  def newHeap[Req, Rep](
    factories: Activity[Set[ServiceFactory[Req, Rep]]],
    statsReceiver: StatsReceiver = NullStatsReceiver,
    emptyException: Throwable = new NoBrokersAvailableException,
    rng: Random = new Random
  ): ServiceFactory[Req, Rep] = new HeapBalancer(
    factories, statsReceiver, emptyException, rng)

  def newAperture[Req, Rep](
    activity: Activity[Traversable[(ServiceFactory[Req, Rep], Double)]],
    smoothWin: Duration = 5.seconds,
    lowLoad: Double = 0.5,
    highLoad: Double = 2,
    maxEffort: Int = 5,
    rng: Rng = Rng.threadLocal,
    timer: Timer = DefaultTimer.twitter,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    emptyException: NoBrokersAvailableException = new NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] = new ApertureLoadBandBalancer(
      activity, smoothWin, lowLoad, highLoad, maxEffort, rng,
      timer, statsReceiver, emptyException)
}
