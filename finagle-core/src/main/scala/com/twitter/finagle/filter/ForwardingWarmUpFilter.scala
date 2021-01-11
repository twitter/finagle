package com.twitter.finagle.filter

import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver, Stat}
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.util.{Future, Duration, Promise, Time}
import scala.util.Random

/**
 * A filter that forwards thrift requests to a specified service. It enables a smooth
 * start of processing the requests by slowly increasing the percentage of requests
 * that are being processed and lowering the amount of requests that are being
 * forwarded.
 */
abstract class ForwardingWarmUpFilter[Req, Rep](
  warmupPeriod: Duration,
  forwardTo: Service[Req, Rep],
  statsReceiver: StatsReceiver = DefaultStatsReceiver)
    extends SimpleFilter[Req, Rep] {

  @volatile private[this] var warmupComplete = false

  private[this] lazy val startTime = Time.now

  private[this] val rng = new Random(0)

  private[this] val scopedStatsReceiver = statsReceiver.scope("warmup")

  private[this] val localScope = scopedStatsReceiver.scope("local")
  private[this] val localLatency = localScope.stat("latency_ms")
  private[this] val localFailureCounter = localScope.counter("failures")

  private[this] val forwardScope = scopedStatsReceiver.scope("forward")
  private[this] val forwardLatency = forwardScope.stat("latency_ms")
  private[this] val forwardFailureCounter = forwardScope.counter("failures")

  private[this] val onWarmup: Promise[Unit] = Promise[Unit]()

  val onWarm: Future[Unit] = onWarmup

  /**
   * Indicates whether the request may be forwarded (i.e. in the case where
   * `bypassForward` is false) or must be handled locally (`bypassForward` is true).
   */
  def bypassForward: Boolean

  final override def apply(request: Req, service: Service[Req, Rep]) = {
    if (warmupComplete || bypassForward) {
      service(request)
    } else {
      val start = startTime.inMillis

      val timePassed = math.max(Time.now.inMillis - start, 0)
      val percentWarm = math.pow(timePassed.toFloat / warmupPeriod.inMillis, 3)

      if (percentWarm >= 1) {
        warmupComplete = true
        onWarmup.setDone()
        service(request)
      } else {
        val r = rng.nextFloat()
        if (percentWarm > r) {
          Stat.timeFuture(localLatency)(service(request)) onFailure { _ =>
            localFailureCounter.incr()
          }
        } else {
          Stat.timeFuture(forwardLatency)(forwardTo(request)) onFailure { _ =>
            forwardFailureCounter.incr()
          }
        }
      }
    }
  }
}
