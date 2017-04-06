package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.{PeakEwma, Updating}
import com.twitter.finagle.{NoBrokersAvailableException, ServiceFactory}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util.{Activity, Duration}

/**
 * Aperture (which is backed by the theory behind p2c) along with the [[PeakEwma]]
 * load metric.
 */
private[loadbalancer] class AperturePeakEwma[Req, Rep](
    protected val endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
    protected val smoothWin: Duration,
    protected val decayTime: Duration,
    protected val lowLoad: Double,
    protected val highLoad: Double,
    protected val minAperture: Int,
    protected val maxEffort: Int,
    protected val rng: Rng,
    protected val statsReceiver: StatsReceiver,
    protected val emptyException: NoBrokersAvailableException,
    protected val useDeterministicOrdering: Boolean)
  extends Aperture[Req, Rep]
  with PeakEwma[Req, Rep]
  with LoadBand[Req, Rep]
  with Updating[Req, Rep] {
  require(minAperture > 0, s"minAperture must be > 0, but was $minAperture")
  protected[this] val maxEffortExhausted = statsReceiver.counter("max_effort_exhausted")
}