package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.{Balancer, Updating}
import com.twitter.finagle.{NoBrokersAvailableException, ServiceFactory}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util.{Activity, Duration}

/**
 * The aperture load-band balancer balances load to the smallest
 * subset ("aperture") of services so that the concurrent load to each service,
 * measured over a window specified by `smoothWin`, stays within the
 * load band delimited by `lowLoad` and `highLoad`.
 *
 * Unavailable services are not counted--the aperture expands as
 * needed to cover those that are available.
 *
 * For example, if the load band is [0.5, 2], the aperture will be
 * adjusted so that no service inside the aperture has a load less
 * than 0.5 or more than 2, so long as offered load permits it.
 *
 * The default load band, [0.5, 2], matches closely the load distribution
 * given by least-loaded without any aperturing.
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
 */
private[loadbalancer] class ApertureLeastLoaded[Req, Rep](
    protected val endpoints: Activity[Traversable[ServiceFactory[Req, Rep]]],
    protected val smoothWin: Duration,
    protected val lowLoad: Double,
    protected val highLoad: Double,
    protected val minAperture: Int,
    protected val maxEffort: Int,
    protected val rng: Rng,
    protected val statsReceiver: StatsReceiver,
    protected val emptyException: NoBrokersAvailableException)
  extends Balancer[Req, Rep]
  with Aperture[Req, Rep]
  with LoadBand[Req, Rep]
  with Updating[Req, Rep] {
  require(minAperture > 0, s"minAperture must be > 0, but was $minAperture")
  protected[this] val maxEffortExhausted = statsReceiver.counter("max_effort_exhausted")
}