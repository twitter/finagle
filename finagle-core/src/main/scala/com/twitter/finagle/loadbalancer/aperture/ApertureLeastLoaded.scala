package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.{EndpointFactory, LeastLoaded, Updating}
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.finagle.{NoBrokersAvailableException, ServiceFactoryProxy}
import com.twitter.util.{Activity, Duration, Future, Time, Timer}

/**
 * Aperture (which is backed by the theory behind p2c) along with the [[LeastLoaded]]
 * load metric.
 */
private[loadbalancer] final class ApertureLeastLoaded[Req, Rep](
  protected val endpoints: Activity[IndexedSeq[EndpointFactory[Req, Rep]]],
  protected val smoothWin: Duration,
  protected val lowLoad: Double,
  protected val highLoad: Double,
  private[aperture] val minAperture: Int,
  protected val maxEffort: Int,
  private[aperture] val rng: Rng,
  protected val statsReceiver: StatsReceiver,
  protected val label: String,
  protected val timer: Timer,
  protected val emptyException: NoBrokersAvailableException,
  protected val useDeterministicOrdering: Option[Boolean],
  private[aperture] val eagerConnections: Boolean,
  private[aperture] val manageEndpoints: Boolean)
    extends Aperture[Req, Rep]
    with LeastLoaded[Req, Rep]
    with LoadBand[Req, Rep]
    with Expiration[Req, Rep]
    with Updating[Req, Rep] {
  require(minAperture > 0, s"minAperture must be > 0, but was $minAperture")
  protected[this] val maxEffortExhausted: Counter = statsReceiver.counter("max_effort_exhausted")

  // We set the idle time as a function of the aperture's smooth window.
  // The aperture growth is dampened by this window so after X windows
  // have passed, we can be sufficiently confident that an idle session
  // is no longer needed. We choose a default of 10 for X which should
  // give us a high degree of confidence and, based on the default smooth
  // windows, should be on the order of minutes.
  protected val endpointIdleTime: Duration = smoothWin * 10

  private[this] val expiryTask = newExpiryTask(timer)

  case class Node(factory: EndpointFactory[Req, Rep])
      extends ServiceFactoryProxy[Req, Rep](factory)
      with LeastLoadedNode
      with LoadBandNode
      with ExpiringNode
      with ApertureNode[Req, Rep] {
    override def tokenRng: Rng = rng
  }

  protected def newNode(factory: EndpointFactory[Req, Rep]): Node = Node(factory)

  override def close(deadline: Time): Future[Unit] = {
    expiryTask.cancel()
    super.close(deadline)
  }

}
