package com.twitter.finagle.loadbalancer

import com.twitter.conversions.time._
import com.twitter.finagle.loadbalancer.aperture.{ApertureLeastLoaded, AperturePeakEwma}
import com.twitter.finagle.loadbalancer.heap.HeapLeastLoaded
import com.twitter.finagle.loadbalancer.p2c.{P2CPeakEwma, P2CLeastLoaded}
import com.twitter.finagle.loadbalancer.roundrobin.RoundRobinBalancer
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy, NoBrokersAvailableException}
import com.twitter.util.{Activity, Duration, Future, Time, Stopwatch}
import scala.util.Random

/**
 * Constructor methods for various load balancers. The methods take balancer
 * specific parameters and return a [[LoadBalancerFactory]] that allows you
 * to easily inject a balancer into the Finagle client stack via the
 * `withLoadBalancer` method.
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
   * This is the default for the fixed number of retries an LB implementation is willing
   * to make if an unavailable (Status != Open) node is returned from the underlying pick.
   *
   * For randomized LBs (P2C* and Aperture*) this can be interpreted as a probability.
   * For example, imagine that half of the replica set is down, the probability of
   * picking two unavailable nodes is 0.25. If we repeat that process for 5 times,
   * the total probability of seeing 5 unavailable nodes in a row, will be
   * (0.25 ^ 5) = 0.1%. This means that if half of the cluster is down, the
   * LB will be making a bad choice (when better choice may have been available)
   * for 0.1% of requests.
   *
   * Please, note that this doesn't mean that 0.1% of requests will be failed
   * by P2C operating on a half-dead cluster since Finagle clients have additional
   * layers of requeues above the load balancer.
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
   * An O(1), concurrent, least-loaded fair load balancer. This uses the ideas
   * behind "power of 2 choices" [1].
   *
   * @param maxEffort This is the fixed number of retries the LB is willing to make
   * if an unavailable node (Status != Open) is returned from the underlying pick.
   * See the constant [[MaxEffort]] for more details on how we pick the default.
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
    override def toString: String = "P2CLeastLoaded"
    def newBalancer[Req, Rep](
      endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] =
      newScopedBal(sr, "p2c_least_loaded",
        new P2CLeastLoaded(endpoints, maxEffort, rng, sr, exc))
  }

  /**
   * Like [[p2c]] but using the Peak EWMA (exponentially weight moving average)
   * load metric.
   *
   * Peak EWMA uses a moving average over an endpoint's round-trip time (RTT) that is
   * highly sensitive to peaks. This average is then weighted by the number of outstanding
   * requests. Effectively, increasing our resolution per-request and providing a higher
   * fidelity measurement of server responsiveness compared to the standard least loaded.
   * It is designed to react to slow endpoints more quickly than least-loaded by penalizing
   * them when they exhibit slow response times. This load metric operates under the
   * assumption that a loaded endpoint takes time to recover and so it is generally safe
   * for the advertised load to incorporate an endpoint's history. However, this
   * assumption breaks down in the presence of long polling clients.
   *
   * @param decayTime The window of latency observations.
   *
   * @param maxEffort This is the fixed number of retries the LB is willing to make
   * if an unavailable node (Status != Open) is returned from the underlying pick.
   * See the constant [[MaxEffort]] for more details on how we pick the default.
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
    override def toString: String = "P2CPeakEwma"
    def newBalancer[Req, Rep](
      endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] =
      newScopedBal(sr, "p2c_peak_ewma",
        new P2CPeakEwma(endpoints, decayTime, Stopwatch.systemNanos, maxEffort, rng, sr, exc))
  }

  /**
   * An efficient strictly least-loaded balancer that maintains an internal heap to
   * select least-loaded endpoints.
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#heap-least-loaded user guide]]
   * for more details.
   */
  def heap(rng: Random = new Random): LoadBalancerFactory =
    new LoadBalancerFactory {
      override def toString: String = "HeapLeastLoaded"
      def newBalancer[Req, Rep](
        endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
        sr: StatsReceiver,
        exc: NoBrokersAvailableException
      ): ServiceFactory[Req, Rep] = {
        newScopedBal(sr, "heap_least_loaded",
          new HeapLeastLoaded(endpoints, sr, exc, rng))
      }
    }

  /**
   * The aperture load-band balancer balances load to the smallest subset ("aperture") of
   * services so that the concurrent load to each service, measured over a window specified
   * by `smoothWin`, stays within the load band delimited by `lowLoad` and `highLoad`.
   *
   * For example, if the load band is [0.5, 2], the aperture will be adjusted so that no
   * service inside the aperture has a load less than 0.5 or more than 2, so long as
   * offered load (or concurrency) permits it.
   *
   * The default load band, [0.5, 2], matches closely the load distribution
   * given by least-loaded without any aperture windowing.
   *
   * Among the benefits of aperture balancing are:
   *
   *  1. A client uses resources commensurate to offered load. In particular,
   *     it does not have to open sessions with every service in a large cluster.
   *     This is especially important when offered load and cluster capacity
   *     are mismatched.
   *  2. It balances over fewer, and thus warmer, services. This enhances the
   *     efficacy of the fail-fast mechanisms, etc. This also means that clients pay
   *     the penalty of session establishment less frequently.
   *  3. It increases the efficacy of least-loaded balancing which, in order to
   *     work well, requires concurrent load. The load-band balancer effectively
   *     arranges load in a manner that ensures a higher level of per-service
   *     concurrency.
   *
   * @param smoothWin The time window to use when calculating the rps observed by this
   * load balancer instance. The smoothed rps value is then used to determine the size of
   * the aperture (along with the `lowLoad` and `highLoad` parameters).
   *
   * @param lowLoad The lower threshold on average load (as calculated by rps over
   * smooth window / # of endpoints). Once this threshold is reached, the aperture is
   * narrowed. Put differently, if there is an average of `lowLoad` requests across
   * the endpoints, then we are not concentrating our concurrency well, so we narrow
   * the aperture.
   *
   * @param highLoad The upper threshold on average load (as calculated by rps / #
   * of instances). Once this threshold is reached, the aperture is widened. Put differently,
   * if there is an average of `highLoad` requests across the endpoints, then we are
   * over subscribing the endpoints and need to widen the aperture.
   *
   * @param minAperture The minimum aperture allowed. Note, this value is checked to
   * ensure that it is not larger than the number of endpoints.
   *
   * @param maxEffort This is the fixed number of retries the LB is willing to make
   * if an unavailable node (Status != Open) is returned from the underlying pick.
   * See the constant [[MaxEffort]] for more details on how we pick the default.
   *
   * @param rng The PRNG used for flipping coins. Override for
   * deterministic tests.
   *
   * @param useDeterministicOrdering Enables the aperture instance to make use of
   * the coordinate in [[DeterministicOrdering]] to calculate an order for the
   * endpoints. In short, this allows coordination for apertures across process
   * boundaries to avoid load concentration when deployed in a distributed system.
   * See [[DeterministicOrdering]] for more details.
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
    rng: Rng = Rng.threadLocal,
    useDeterministicOrdering: Boolean = false
  ): LoadBalancerFactory = new LoadBalancerFactory {
    override def toString: String = "ApertureLeastLoaded"
    def newBalancer[Req, Rep](
      endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] = {
      newScopedBal(sr, "aperture_least_loaded",
        new ApertureLeastLoaded(endpoints, smoothWin, lowLoad,
          highLoad, minAperture, maxEffort, rng, sr, exc, useDeterministicOrdering))
    }
  }

  /**
   * Like [[aperture]] but but using the Peak EWMA (exponentially weight moving average)
   * load metric.
   *
   * Peak EWMA uses a moving average over an endpoint's round-trip time (RTT) that is
   * highly sensitive to peaks. This average is then weighted by the number of outstanding
   * requests. Effectively, increasing our resolution per-request and providing a higher
   * fidelity measurement of server responsiveness compared to the standard least loaded.
   * It is designed to react to slow endpoints more quickly than least-loaded by penalizing
   * them when they exhibit slow response times. This load metric operates under the
   * assumption that a loaded endpoint takes time to recover and so it is generally safe
   * for the advertised load to incorporate an endpoint's history. However, this
   * assumption breaks down in the presence of long polling clients.
   *
   * @param smoothWin The time window to use when calculating the rps observed by this
   * load balancer instance. The smoothed rps value is used to determine the size of
   * aperture (along with the lowLoad and highLoad parameters). In the context of
   * peakEwma, this value is also used to average over the latency observed per endpoint.
   * It's unlikely that you would want to measure the latency on a different time scale
   * than rps, so we couple the two.
   *
   * @param lowLoad The lower threshold on average load (as calculated by rps over
   * smooth window / # of endpoints). Once this threshold is reached, the aperture is
   * narrowed. Put differently, if there is an average of `lowLoad` requests across
   * the endpoints, then we are not concentrating our concurrency well, so we narrow
   * the aperture.
   *
   * @param highLoad The upper threshold on average load (as calculated by rps / #
   * of instances). Once this threshold is reached, the aperture is widened. Put differently,
   * if there is an average of `highLoad` requests across the endpoints, then we are
   * over subscribing the endpoints and need to widen the aperture.
   *
   * @param minAperture The minimum aperture allowed. Note, this value is checked to
   * ensure that it is not larger than the number of endpoints.
   *
   * @param maxEffort This is the fixed number of retries the LB is willing to make
   * if an unavailable node (Status != Open) is returned from the underlying pick.
   * See the constant [[MaxEffort]] for more details on how we pick the default.
   *
   * @param rng The PRNG used for flipping coins. Override for
   * deterministic tests.
   *
   * @param useDeterministicOrdering Enables the aperture instance to make use of
   * the coordinate in [[DeterministicOrdering]] to calculate an order for the
   * endpoints. In short, this allows coordination for apertures across process
   * boundaries to avoid load concentration when deployed in a distributed system.
   * See [[DeterministicOrdering]] for more details.
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#aperture-least-loaded user guide]]
   * for more details.
   */
  def aperturePeakEwma(
    smoothWin: Duration = 15.seconds,
    lowLoad: Double = 0.5,
    highLoad: Double = 2.0,
    minAperture: Int = 1,
    maxEffort: Int = MaxEffort,
    rng: Rng = Rng.threadLocal,
    useDeterministicOrdering: Boolean = false
  ): LoadBalancerFactory = new LoadBalancerFactory {
    override def toString: String = "AperturePeakEwma"
    def newBalancer[Req, Rep](
      endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] = {
      newScopedBal(sr, "aperture_peak_ewma",
        new AperturePeakEwma(endpoints, smoothWin, smoothWin, Stopwatch.systemNanos, lowLoad,
          highLoad, minAperture, maxEffort, rng, sr, exc, useDeterministicOrdering))
    }
  }

  /**
   * A simple round robin balancer that chooses the next endpoint in the list
   * for each request.
   *
   * WARNING: Unlike other balancers available in finagle, this does
   * not take latency into account and will happily direct load to
   * slow or oversubscribed services. We recommend using one of the
   * other load balancers for typical production use.
   *
   * @param maxEffort This is the fixed number of retries the LB is willing to make
   * if an unavailable node (Status != Open) is returned from the underlying pick.
   * See the constant [[MaxEffort]] for more details on how we pick the default.
   */
  def roundRobin(
    maxEffort: Int = MaxEffort
  ): LoadBalancerFactory = new LoadBalancerFactory {
    override def toString: String = "RoundRobin"
    def newBalancer[Req, Rep](
      endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
      sr: StatsReceiver,
      exc: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] = {
      newScopedBal(sr, "round_robin", new RoundRobinBalancer(endpoints, sr, exc))
    }
  }
}
