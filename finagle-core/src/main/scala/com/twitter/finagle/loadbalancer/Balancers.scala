package com.twitter.finagle.loadbalancer

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.{Rng, DefaultTimer}
import com.twitter.finagle.{ServiceFactory, NoBrokersAvailableException}
import com.twitter.util.{Activity, Duration, Future, Timer, Time}
import scala.util.Random

/**
 * Constructor methods for various load balancers. The methods take balancer
 * specific parameters and return a [[LoadBalancerFactory]] that allows you
 * to easily inject a balancer into the Finagle stack via client configuration.
 */
object Balancers {
  /** Default MaxEffort used in constructors below. */
  val MaxEffort: Int = 5

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
    def newBalancer[Req, Rep](
      endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] =
      new P2CBalancer(endpoints, maxEffort, rng, sr, exc) {
        private[this] val gauge = sr.addGauge("p2c")(1)
      }
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
   */
  def p2cPeakEwma(
    decayTime: Duration = 10.seconds,
    maxEffort: Int = MaxEffort,
    rng: Rng = Rng.threadLocal
  ): LoadBalancerFactory = new LoadBalancerFactory {
    def newBalancer[Req, Rep](
      endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] =
      new P2CBalancerPeakEwma(endpoints, decayTime, maxEffort, rng, sr, exc) {
        private[this] val gauge = sr.addGauge("p2cPeakEwma")(1)
        override def close(when: Time): Future[Unit] = {
          gauge.remove()
          super.close(when)
        }
      }
  }

  /**
   * An efficient strictly least-loaded balancer that maintains
   * an internal heap. Note, because weights are not supported by
   * the HeapBalancer they are ignored when the balancer is constructed.
   */
  def heap(rng: Random = new Random): LoadBalancerFactory =
    new LoadBalancerFactory {
      def newBalancer[Req, Rep](
        endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
        sr: StatsReceiver,
        exc: NoBrokersAvailableException
      ): ServiceFactory[Req, Rep] = {
        new HeapBalancer(endpoints, sr, exc, rng) {
          private[this] val gauge = sr.addGauge("heap")(1)
          override def close(when: Time): Future[Unit] = {
            gauge.remove()
            super.close(when)
          }
        }
      }
    }

  /**
   * The aperture load-band balancer balances load to the smallest
   * subset ("aperture") of services so that:
   *
   *  1. The concurrent load, measured over a window specified by
   *     `smoothWin`, to each service stays within the load band, delimited
   *     by `lowLoad` and `highLoad`.
   *  2. Services receive load proportional to the ratio of their
   *     weights.
   *
   * Unavailable services are not counted--the aperture expands as
   * needed to cover those that are available.
   */
  def aperture(
    smoothWin: Duration = 5.seconds,
    lowLoad: Double = 0.5,
    highLoad: Double = 2,
    minAperture: Int = 1,
    timer: Timer = DefaultTimer.twitter,
    maxEffort: Int = MaxEffort,
    rng: Rng = Rng.threadLocal
  ): LoadBalancerFactory = new LoadBalancerFactory {
    def newBalancer[Req, Rep](
      endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] = {
      new ApertureLoadBandBalancer(endpoints, smoothWin, lowLoad,
        highLoad, minAperture, maxEffort, rng, timer, sr, exc) {
        private[this] val gauge = sr.addGauge("aperture")(1)
        override def close(when: Time): Future[Unit] = {
          gauge.remove()
          super.close(when)
        }
      }
    }
  }
}
