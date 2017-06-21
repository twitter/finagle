package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.netty4.util.Netty4Timer
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Time}
import java.util.concurrent.TimeUnit

/**
 * Configures `ticksPerWheel` on the singleton instance of `Netty4Timer`.
 */
private object timerTicksPerWheel extends GlobalFlag[Int](
  512,
  "Netty 4 timer ticks per wheel")

/**
 * Configures `tickDuration` on the singleton instance of `Netty4Timer`.
 */
private object timerTickDuration extends GlobalFlag[Duration](
  10.milliseconds,
  "Netty 4 timer tick duration")

/**
 * A default instance of Netty timer that needs to be shared between [[Netty4HashedWheelTimer]]
 * object (used in protocols) and instances (service-loaded).
 *
 */
private object hashedWheelTimer extends io.netty.util.HashedWheelTimer(
  new NamedPoolThreadFactory("Netty 4 Timer", /*daemon = */true),
  timerTickDuration().inMilliseconds, TimeUnit.MILLISECONDS,
  timerTicksPerWheel(),
  /*leakDetection = */false) { self =>

  private[this] val statsPollInterval = 10.seconds

  private object deviationStat extends io.netty.util.TimerTask {

    private[this] val tickDuration = timerTickDuration()
    private[this] val deviationMs = FinagleStatsReceiver.stat("timer", "deviation_ms")
    private[this] var nextAt = Time.now + tickDuration

    def run(timeout: io.netty.util.Timeout): Unit = {
      val now = Time.now
      val delta = now - nextAt
      nextAt = now + tickDuration
      deviationMs.add(delta.inMilliseconds)

      self.newTimeout(this, tickDuration.inMilliseconds, TimeUnit.MILLISECONDS)
    }
  }

  private object pendingTasksStat extends io.netty.util.TimerTask {
    private[this] val pendingTasks = FinagleStatsReceiver.stat("timer", "pending_tasks")

    def run(timeout: io.netty.util.Timeout): Unit = {
      pendingTasks.add(self.pendingTimeouts)
      self.newTimeout(pendingTasksStat, statsPollInterval.inSeconds, TimeUnit.SECONDS)
    }
  }

  self.newTimeout(deviationStat, timerTickDuration().inMilliseconds, TimeUnit.MILLISECONDS)
  self.newTimeout(pendingTasksStat, statsPollInterval.inSeconds, TimeUnit.SECONDS)
}

/**
 * A default implementation of [[Netty4Timer]] that's based on `HashedWheelTimer` and uses
 * the default `ticksPerWheel` size of 512 and 10 millisecond ticks, which gives ~5100
 * milliseconds worth of scheduling. This should suffice for most usage without having
 * tasks scheduled for a later round.

 * This class is intended to be service-loaded instead of directly instantiated.
 * See [[com.twitter.finagle.util.LoadService]] and [[com.twitter.finagle.util.DefaultTimer]].
 *
 * This timer also exports some stats under `finagle/timer` (see [[FinagleStatsReceiver]]):
 *
 * 1. `deviation_ms`
 * 2. `pending_tasks`
 *
 * To configure this timer use the following CLI flags:
 *
 * 1. `-com.twitter.finagle.netty4.timerTickDuration=100.milliseconds`
 * 2. `-com.twitter.finagle.netty4.timerTicksPerWheel=512`
 */
private[finagle] class Netty4HashedWheelTimer extends Netty4Timer(hashedWheelTimer) {

  private[this] val log = Logger.get()

  // This timer is "unstoppable".
  override def stop(): Unit =
    log.warning(s"Ignoring call to `Timer.stop()` on an unstoppable Netty4Timer.\n" +
      s"Current stack trace: ${ Thread.currentThread.getStackTrace.mkString("\n") }")
}

/**
 * A singleton instance of [[Netty4HashedWheelTimer]].
 */
private[finagle] object Netty4HashedWheelTimer extends Netty4HashedWheelTimer
