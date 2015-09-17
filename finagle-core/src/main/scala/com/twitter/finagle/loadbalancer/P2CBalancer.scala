package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util._

/**
 * An O(1), concurrent, weighted least-loaded fair load balancer.
 * This uses the ideas behind "power of 2 choices" [1] combined with
 * O(1) biased coin flipping through the aliasing method, described
 * in [[com.twitter.finagle.util.Drv Drv]].
 *
 * @param activity An activity that updates with the set of
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
private class P2CBalancer[Req, Rep](
    protected val activity: Activity[Traversable[ServiceFactory[Req, Rep]]],
    protected val maxEffort: Int,
    protected val rng: Rng,
    protected val statsReceiver: StatsReceiver,
    protected val emptyException: NoBrokersAvailableException)
  extends Balancer[Req, Rep]
  with LeastLoaded[Req, Rep]
  with P2C[Req, Rep]
  with Updating[Req, Rep]

/**
 * Like [[com.twitter.finagle.loadbalancer.P2CBalancer]] but
 * using the Peak EWMA load metric.
 *
 * Peak EWMA is designed to converge quickly when encountering
 * slow endpoints. It is quick to react to latency spikes, recovering
 * only cautiously. Peak EWMA takes history into account, so that
 * slow behavior is penalized relative to the supplied decay time.
 *
 * @param activity An activity that updates with the set of
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
private class P2CBalancerPeakEwma[Req, Rep](
    protected val activity: Activity[Traversable[ServiceFactory[Req, Rep]]],
    protected val decayTime: Duration,
    protected val maxEffort: Int,
    protected val rng: Rng,
    protected val statsReceiver: StatsReceiver,
    protected val emptyException: NoBrokersAvailableException)
  extends Balancer[Req, Rep]
  with PeakEwma[Req, Rep]
  with P2C[Req, Rep]
  with Updating[Req, Rep]

private trait PeakEwma[Req, Rep] { self: Balancer[Req, Rep] =>

  protected def rng: Rng

  protected def decayTime: Duration

  protected def nanoTime(): Long = System.nanoTime()

  protected class Metric(sr: StatsReceiver, name: String) {
    private[this] val epoch = nanoTime()
    private[this] val Penalty: Double = Double.MaxValue/2
    // The mean lifetime of `cost`, it reaches its half-life after Tau*ln(2).
    private[this] val Tau: Double = decayTime.inNanoseconds.toDouble
    require(Tau > 0)

    // these are all guarded by synchronization on `this`
    private[this] var stamp: Long = epoch   // last timestamp in nanos we observed an rtt
    private[this] var pending: Int = 0      // instantaneous rate
    private[this] var cost: Double = 0.0    // ewma of rtt, sensitive to peaks.

    def rate(): Int = synchronized { pending }

    // Calculate the exponential weighted moving average of our
    // round trip time. It isn't exactly an ewma, but rather a
    // "peak-ewma", since `cost` is hyper-sensitive to latency peaks.
    // Note, because the frequency of observations represents an
    // unevenly spaced time-series[1], we consider the time between
    // observations when calculating our weight.
    // [1] http://www.eckner.com/papers/ts_alg.pdf
    private[this] def observe(rtt: Double): Unit = {
      val t = nanoTime()
      val td = math.max(t-stamp, 0)
      val w = math.exp(-td/Tau)
      if (rtt > cost) cost = rtt
      else cost = cost*w + rtt*(1.0-w)
      stamp = t
    }

    def get(): Double = synchronized {
      // update our view of the decay on `cost`
      observe(0.0)

      // If we don't have any latency history, we penalize the host on
      // the first probe. Otherwise, we factor in our current rate
      // assuming we were to schedule an additional request.
      if (cost == 0.0 && pending != 0) Penalty+pending
      else cost*(pending+1)
    }

    def start(): Long = synchronized {
      pending += 1
      nanoTime()
    }

    def end(ts: Long): Unit = synchronized {
      val rtt = math.max(nanoTime()-ts, 0)
      pending -= 1
      observe(rtt)
    }
  }

  protected case class Node(factory: ServiceFactory[Req, Rep], metric: Metric, token: Int)
      extends ServiceFactoryProxy[Req, Rep](factory)
      with NodeT {
    type This = Node

    def load = metric.get()
    def pending = metric.rate()

    override def apply(conn: ClientConnection) = {
      val ts = metric.start()
      super.apply(conn) transform {
        case Return(svc) =>
          Future.value(new ServiceProxy(svc) {
            override def close(deadline: Time) =
              super.close(deadline) ensure {
                metric.end(ts)
              }
          })

        case t@Throw(_) =>
          metric.end(ts)
          Future.const(t)
      }
    }
  }

  protected def newNode(factory: ServiceFactory[Req, Rep], statsReceiver: StatsReceiver): Node =
    Node(factory, new Metric(statsReceiver, factory.toString), rng.nextInt())

  protected def failingNode(cause: Throwable) = Node(
    new FailingFactory(cause),
    new Metric(NullStatsReceiver, "failing"),
    0
  )
}
