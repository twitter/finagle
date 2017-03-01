package com.twitter.finagle.loadbalancer

import com.twitter.conversions.time._
import com.twitter.finagle.loadbalancer.aperture.ApertureLeastLoaded
import com.twitter.finagle.loadbalancer.heap.HeapLeastLoaded
import com.twitter.finagle.loadbalancer.p2c.{P2CPeakEwma, P2CLeastLoaded}
import com.twitter.finagle.loadbalancer.roundrobin.RoundRobinBalancer
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy, NoBrokersAvailableException}
import com.twitter.util.{Activity, Duration, Future, Time}
import scala.util.Random

/**
 * Constructor methods for various load balancers. The methods take balancer
 * specific parameters and return a [[LoadBalancerFactory]] that allows you
 * to easily inject a balancer into the Finagle stack via client configuration.
 *
 * @example configuring a client with a load balancer
 * {{{
 * $Protocol.client
 *   .withLoadBalancer(Balancers.aperture())
 *   .newClient(...)
 * }}}
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing user guide]]
 * for more details.
 */
object Balancers {

  /**
   * This is a fixed number of retries, a LB are willing to make if the dead
   * node is returned from the underlying distributor.
   *
   * For randomized LBs (P2C/LeastLoaded, P2C/EWMA, and Aperture) this value
   * has an additional meaning: it determines how often the LB will be failing
   * to pick a healthy node out of the partially-unhealthy replica set. For
   * example, imagine that half of the replica set is down, the probably of
   * picking two dead nodes is 0.25. If we repeat that process for 5 times,
   * the total probability of seeing 5 dead nodes in a row, will be
   * (0.25 ^ 5) = 0.1%. This means that if half of the cluster is down, the
   * LB will be making a bad choice (when better choice may have been available)
   * for 0.1% of requests.
   *
   * Please, note that this doesn't mean that 0.1% of requests will be failed
   * by P2C operating on a half-dead cluster since there is an additional layer
   * of requeues (see `Retries` module) involved above those "bad picks".
   */
  val MaxEffort: Int = 5

  /**
   * Creates a [[ServiceFactory]] proxy to `bal` with the `lbType` exported
   * to a gauge.
   */
  private def newScopedBal[Req, Rep](
    sr: StatsReceiver,
    lbType: String,
    bal: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] =
    new ServiceFactoryProxy(bal) {
      private[this] val typeGauge = sr.scope("algorithm").addGauge(lbType)(1)
      override def close(when: Time): Future[Unit] = {
        typeGauge.remove()
        super.close(when)
      }
    }

  /**
   * An O(1), concurrent, weighted least-loaded fair load balancer.
   * This uses the ideas behind "power of 2 choices" [1] combined with
   * O(1) biased coin flipping through the aliasing method, described
   * in [[com.twitter.finagle.util.Drv Drv]].
   *
   * @param maxEffort the maximum amount of "effort" we're willing to
   * expend on a load balancing decision without reweighing.
   *
   * @param rng The PRNG used for flipping coins. Override for
   * deterministic tests.
   *
   * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
   * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
   * 10 (October 2001), 1094-1104.
   */
  def p2c(
    maxEffort: Int = MaxEffort,
    rng: Rng = Rng.threadLocal
  ): LoadBalancerFactory = new LoadBalancerFactory {
    override def toString: String = "P2cLoadBalancerFactory"
    def newBalancer[Req, Rep](
      endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] =
      newScopedBal(sr, "p2c_least_loaded",
        new P2CLeastLoaded(endpoints, maxEffort, rng, sr, exc))
  }

  /**
   * Like [[p2c]] but using the Peak EWMA load metric.
   *
   * Peak EWMA uses a moving average over an endpoint's round-trip time (RTT) that is
   * highly sensitive to peaks. This average is then weighted by the number of outstanding
   * requests, effectively increasing our resolution per-request. It is designed to react
   * to slow endpoints more quickly than least-loaded by penalizing them when they exhibit
   * slow response times. This load metric operates under the assumption that a loaded
   * endpoint takes time to recover and so it is generally safe for the advertised load
   * to incorporate an endpoint's history. However, this assumption breaks down in the
   * presence of long polling clients.
   *
   * @param decayTime The window of latency observations.
   *
   * @param maxEffort the maximum amount of "effort" we're willing to
   * expend on a load balancing decision without reweighing.
   *
   * @param rng The PRNG used for flipping coins. Override for
   * deterministic tests.
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#power-of-two-choices-p2c-least-loaded user guide]]
   * for more details.
   */
  def p2cPeakEwma(
    decayTime: Duration = 10.seconds,
    maxEffort: Int = MaxEffort,
    rng: Rng = Rng.threadLocal
  ): LoadBalancerFactory = new LoadBalancerFactory {
    override def toString: String = "P2cPeakEwmaLoadBalancerFactory"
    def newBalancer[Req, Rep](
      endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] =
      newScopedBal(sr, "p2c_peak_ewma",
        new P2CPeakEwma(endpoints, decayTime, maxEffort, rng, sr, exc))
  }

  /**
   * An efficient strictly least-loaded balancer that maintains
   * an internal heap to select least-loaded endpoints.
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#heap-least-loaded user guide]]
   * for more details.
   */
  def heap(rng: Random = new Random): LoadBalancerFactory =
    new LoadBalancerFactory {
      override def toString: String = "HeapLoadBalancerFactory"
      def newBalancer[Req, Rep](
        endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
        sr: StatsReceiver,
        exc: NoBrokersAvailableException
      ): ServiceFactory[Req, Rep] = {
        newScopedBal(sr, "heap_least_loaded",
          new HeapLeastLoaded(endpoints, sr, exc, rng))
      }
    }

  /**
   * The aperture load-band balancer balances load to the smallest
   * subset ("aperture") of services so that:
   *
   *  1. The concurrent load, measured over a window specified by
   *     `smoothWin`, to each service stays within the load band, delimited
   *     by `lowLoad` and `highLoad`.
   *
   *  2. Services receive load proportional to the ratio of their
   *     weights.
   *
   * Unavailable services are not counted--the aperture expands as
   * needed to cover those that are available.
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#aperture-least-loaded user guide]]
   * for more details.
   */
  def aperture(
    smoothWin: Duration = 5.seconds,
    lowLoad: Double = 0.5,
    highLoad: Double = 2.0,
    minAperture: Int = 1,
    maxEffort: Int = MaxEffort,
    rng: Rng = Rng.threadLocal
  ): LoadBalancerFactory = new LoadBalancerFactory {
    override def toString: String = "ApertureLoadBalancerFactory"
    def newBalancer[Req, Rep](
      endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] = {
      newScopedBal(sr, "aperture_least_loaded",
        new ApertureLeastLoaded(endpoints, smoothWin, lowLoad,
          highLoad, minAperture, maxEffort, rng, sr, exc))
    }
  }

  /**
   * A simple round robin balancer that chooses the next backend in
   * the list for each request.
   *
   * WARNING: Unlike other balancers available in finagle, this does
   * not take latency into account and will happily direct load to
   * slow or oversubscribed services. We recommend using one of the
   * other load balancers for typical production use.
   *
   * @param maxEffort the maximum amount of "effort" we're willing to
   * expend on a load balancing decision without reweighing.
   */
  def roundRobin(
    maxEffort: Int = MaxEffort
  ): LoadBalancerFactory = new LoadBalancerFactory {
    override def toString: String = "RoundRobinLoadBalancerFactory"
    def newBalancer[Req, Rep](
      endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] = {
      newScopedBal(sr, "round_robin", new RoundRobinBalancer(endpoints, sr, exc))
    }
  }
}
