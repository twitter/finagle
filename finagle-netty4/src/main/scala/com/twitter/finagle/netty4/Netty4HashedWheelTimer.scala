package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.netty4.util.Netty4Timer
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Time, Try}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

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
 * A default instance of [[Netty4Timer]] that's based on `HashedWheelTimer` and uses
 * the default `ticksPerWheel` size of 512 and 10 millisecond ticks, which gives ~5100
 * milliseconds worth of scheduling. This should suffice for most usage without having
 * tasks scheduled for a later round.
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
 *
 * @note We override `maxPendingTimeouts` just to the maximum possible value just to make
 *       Netty's timer maintain its internal `pendingTimeouts` counter. We wont' need it
 *       after https://github.com/netty/netty/pull/6682 is merged.
 */
private[finagle] object Netty4HashedWheelTimer extends Netty4Timer(new io.netty.util.HashedWheelTimer(
  new NamedPoolThreadFactory("Netty 4 Timer", /*daemon = */true),
  timerTickDuration().inMilliseconds, TimeUnit.MILLISECONDS,
  timerTicksPerWheel(),
  /*leakDetection = */false,
  /*maxPendingTimeouts = */Long.MaxValue)) { self =>

  private[this] val statsPollInterval = 10.seconds
  private[this] val log = Logger.get()

  private object deviationStat extends Function0[Unit] {
    private[this] val tickDuration = timerTickDuration()
    private[this] val deviationMs = FinagleStatsReceiver.stat("timer", "deviation_ms")
    private[this] var nextAt = Time.now + tickDuration

    def apply(): Unit = {
      val now = Time.now
      val delta = now - nextAt
      nextAt = now + tickDuration
      deviationMs.add(delta.inMilliseconds)

      self.schedule(nextAt)(deviationStat)
    }
  }

  private object pendingTasksStat extends Function0[Unit] {
    private[this] val pendingTasks = FinagleStatsReceiver.stat("timer", "pending_tasks")

    // This represents HashedWheelTimer's private field `pendingTimeouts`.
    // We can remove when https://github.com/netty/netty/pull/6682 is available.
    private[this] val pendingTimeouts: Try[AtomicLong] = Try {
      val field = classOf[io.netty.util.HashedWheelTimer].getDeclaredField("pendingTimeouts")
      field.setAccessible(true)
      field.get(underlying).asInstanceOf[AtomicLong]
    }

    def apply(): Unit = pendingTimeouts.foreach(pt => pendingTasks.add(pt.get()))
  }

  // Start tasks exporting stats.
  self.schedule(Time.now + timerTickDuration())(deviationStat)
  self.schedule(statsPollInterval)(pendingTasksStat)

  // This timer is "unstoppable".
  override def stop(): Unit =
    log.warning(s"Ignoring call to `Timer.stop()` on an unstoppable Netty4Timer.\n" +
      s"Current stack trace: ${ Thread.currentThread.getStackTrace.mkString("\n") }")
}
