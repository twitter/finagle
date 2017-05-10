package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.{LeastLoaded, Updating}
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.{NoBrokersAvailableException, ServiceFactory, ServiceFactoryProxy}
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util.{Activity, Duration}

/**
 * Aperture (which is backed by the theory behind p2c) along with the [[LeastLoaded]]
 * load metric.
 */
private[loadbalancer] final class ApertureLeastLoaded[Req, Rep](
    protected val endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
    protected val smoothWin: Duration,
    protected val lowLoad: Double,
    protected val highLoad: Double,
    protected val minAperture: Int,
    protected val maxEffort: Int,
    protected val rng: Rng,
    protected val statsReceiver: StatsReceiver,
    protected val emptyException: NoBrokersAvailableException,
    protected val useDeterministicOrdering: Boolean)
  extends Aperture[Req, Rep]
  with LeastLoaded[Req, Rep]
  with LoadBand[Req, Rep]
  with Updating[Req, Rep] {
  require(minAperture > 0, s"minAperture must be > 0, but was $minAperture")
  protected[this] val maxEffortExhausted: Counter = statsReceiver.counter("max_effort_exhausted")

  case class Node(factory: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy[Req, Rep](factory)
    with LeastLoadedNode
    with LoadBandNode
    with ApertureNode

  protected def newNode(factory: ServiceFactory[Req, Rep]): Node = Node(factory)
  protected def failingNode(cause: Throwable): Node = Node(new FailingFactory(cause))
}
