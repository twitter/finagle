package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag
import com.twitter.conversions.DurationOps._
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.netty4.util.Netty4Timer
import com.twitter.finagle.stats.{FinagleStatsReceiver, StatsReceiver, Verbosity}
import com.twitter.finagle.util.ServiceLoadedTimer
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Time}
import java.util.concurrent.TimeUnit

/**
 * Configures `ticksPerWheel` on the singleton instance of `HashedWheelTimer`.
 */
private object timerTicksPerWheel extends GlobalFlag[Int](512, "Netty 4 timer ticks per wheel")

/**
 * Configures `tickDuration` on the singleton instance of `HashedWheelTimer`.
 */
private object timerTickDuration
    extends GlobalFlag[Duration](10.milliseconds, "Netty 4 timer tick duration")

/**
 * A Netty timer for use with [[Netty4HashedWheelTimer]].
 */
private class HashedWheelTimer(
  statsReceiver: StatsReceiver,
  tickDuration: Duration,
  ticksPerWheel: Int)
    extends io.netty.util.HashedWheelTimer(
      new NamedPoolThreadFactory("Netty 4 Timer", /*daemon = */ true),
      tickDuration.inMilliseconds,
      TimeUnit.MILLISECONDS,
      ticksPerWheel,
      /*leakDetection = */ false
    ) { self =>

  private[this] val statsPollInterval = 10.seconds

  private object deviationStat extends io.netty.util.TimerTask {

    private[this] val tickDuration = timerTickDuration()
    private[this] val deviationMs = statsReceiver.stat(Verbosity.Debug, "timer", "deviation_ms")
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
    private[this] val pendingTasks = statsReceiver.stat(Verbosity.Debug, "timer", "pending_tasks")

    def run(timeout: io.netty.util.Timeout): Unit = {
      pendingTasks.add(self.pendingTimeouts)
      self.newTimeout(pendingTasksStat, statsPollInterval.inSeconds, TimeUnit.SECONDS)
    }
  }

  self.newTimeout(deviationStat, timerTickDuration().inMilliseconds, TimeUnit.MILLISECONDS)
  self.newTimeout(pendingTasksStat, statsPollInterval.inSeconds, TimeUnit.SECONDS)
}

object HashedWheelTimer {

  /**
   * A singleton instance of [[HashedWheelTimer]] that is used for the all service loaded
   * instances of [[Netty4HashedWheelTimer]]. Configuration is done via global flags.
   *
   * @note Stats are reported into the "finagle" scope.
   */
  private[netty4] val instance: HashedWheelTimer = {
    new HashedWheelTimer(
      FinagleStatsReceiver,
      timerTickDuration(),
      timerTicksPerWheel()
    )
  }

  /**
   * Stop default Netty 4 Timer.
   *
   * @note When timer is stopped, the behaviors of Finagle client and server are undefined.
   */
  def stop(): Unit = {
    Logger
      .get().warning(
        "Stopping the default Finagle Netty 4 Timer. When timer is stopped, " +
          "the behaviors of Finagle client and server are undefined."
      )
    instance.stop()
  }

}

/**
 * A default implementation of [[Netty4Timer]] that's based on `HashedWheelTimer` and uses
 * the default `ticksPerWheel` size of 512 and 10 millisecond ticks, which gives ~5100
 * milliseconds worth of scheduling. This should suffice for most usage without having
 * tasks scheduled for a later round.

 * This class is intended to be service-loaded instead of directly instantiated.
 * See [[com.twitter.finagle.util.LoadService]] and [[com.twitter.finagle.util.DefaultTimer]].
 *
 * This timer also exports metrics under `finagle/timer` (see
 * [[https://twitter.github.io/finagle/guide/Metrics.html#timer metrics documentation]]):
 *
 * 1. `deviation_ms`
 * 2. `pending_tasks`
 *
 * To configure this timer use the following CLI flags:
 *
 * 1. `-com.twitter.finagle.netty4.timerTickDuration=100.milliseconds`
 * 2. `-com.twitter.finagle.netty4.timerTicksPerWheel=512`
 */
private[netty4] class Netty4HashedWheelTimer
    extends Netty4Timer(HashedWheelTimer.instance)
    with ServiceLoadedTimer {

  private[this] val log = Logger.get()

  // This timer is "unstoppable".
  override def stop(): Unit =
    log.warning(
      s"Ignoring call to `Timer.stop()` on an unstoppable Netty4Timer.\n" +
        s"Current stack trace: ${Thread.currentThread.getStackTrace.mkString("\n")}"
    )
}
