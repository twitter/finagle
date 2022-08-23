package com.twitter.finagle.exp.fiber_scheduler.util

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.exp.fiber_scheduler.Config
import com.twitter.finagle.exp.fiber_scheduler.statsReceiver

/**
 * Low resolution clock implementation. It significantly reduces the overhead
 * to measure time by updating the current time regularly instead of returning
 * the precise time.
 */
private[fiber_scheduler] final object LowResClock {

  @volatile private[this] var now = System.nanoTime()

  private[this] val counter = statsReceiver.scope("low_res_clock").counter("refresh")

  private[this] val refresh: Runnable =
    () => {
      counter.incr()
      now = System.nanoTime()
    }

  // use a dedicated thread to reliably update the time
  Executors
    .newScheduledThreadPool(1, new NamedPoolThreadFactory("fiber/lowResClock", makeDaemons = true))
    .scheduleAtFixedRate(
      refresh,
      0,
      Config.Scheduling.lowResClockResolution.inNanoseconds,
      TimeUnit.NANOSECONDS)

  def nowNanos(): Long = now
}
