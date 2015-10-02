package com.twitter.finagle.util

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.conversions.time._
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.util._
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import org.jboss.netty.{util => netty}

// Wrapper around Netty timers.
private class HashedWheelTimer(underlying: netty.Timer) extends Timer {
  def schedule(when: Time)(f: => Unit): TimerTask = {
    val timeout = underlying.newTimeout(new netty.TimerTask {
      val saved = Local.save()
      def run(to: netty.Timeout) {
        if (!to.isCancelled) runInContext(saved, f)
      }
    }, math.max(0, (when - Time.now).inMilliseconds), TimeUnit.MILLISECONDS)
    toTimerTask(timeout)
  }

  def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = new TimerTask {
    val saved = Local.save()
    var isCancelled = false
    var ref: TimerTask = schedule(when) { loop() }

    def loop() {
      runInContext(saved, f)
      synchronized {
        if (!isCancelled) ref = schedule(period.fromNow) { loop() }
      }
    }

    def cancel() {
      synchronized {
        isCancelled = true
        ref.cancel()
      }
    }
  }

  def stop(): Unit = underlying.stop()

  private[this] def toTimerTask(task: netty.Timeout) = new TimerTask {
    def cancel() { task.cancel() }
  }

  private[this] def runInContext(saved: Local.Context, f: => Unit): Unit = {
    Local.restore(saved)
    Monitor(f)
  }
}

/**
 * A HashedWheelTimer that uses [[org.jboss.netty.util.HashedWheelTimer]] under
 * the hood. Prefer using a single instance per application, the default
 * instance is [[Default]].
 */
object HashedWheelTimer {
  /**
   * A wheel size of 512 and 10 millisecond ticks provides approximately 5100
   * milliseconds worth of scheduling. This should suffice for most usage
   * without having tasks scheduled for a later round.
   */
  val TickDuration = 10.milliseconds
  val TicksPerWheel = 512

  /**
   * Create a default `HashedWheelTimer`.
   */
  def apply(): Timer = HashedWheelTimer(
    Executors.defaultThreadFactory(),
    TickDuration,
    TicksPerWheel)

  /**
   * Create a `HashedWheelTimer` with custom [[ThreadFactory]], [[Duration]]
   * and ticks per wheel.
   */
  def apply(threadFactory: ThreadFactory, tickDuration: Duration, ticksPerWheel: Int): Timer = {
    val hwt = new netty.HashedWheelTimer(
      threadFactory,
      tickDuration.inNanoseconds,
      TimeUnit.NANOSECONDS,
      ticksPerWheel)
    new HashedWheelTimer(hwt)
  }

  /**
   * Create a `HashedWheelTimer` with custom [[ThreadFactory]] and [[Duration]].
   */
  def apply(threadFactory: ThreadFactory, tickDuration: Duration): Timer =
    HashedWheelTimer(threadFactory, tickDuration, TicksPerWheel)

  /**
   * Create a `HashedWheelTimer` with custom [[Duration]].
   */
  def apply(tickDuration: Duration): Timer =
    HashedWheelTimer(Executors.defaultThreadFactory(), tickDuration, TicksPerWheel)

  /**
   * Create a `HashedWheelTimer` with custom [[Duration]] and ticks per wheel.
   */
  def apply(tickDuration: Duration, ticksPerWheel: Int): Timer =
    HashedWheelTimer(Executors.defaultThreadFactory(), tickDuration, ticksPerWheel)

  // Note: this uses the default `ticksPerWheel` size of 512 and 10 millisecond
  // ticks, which gives ~5100 milliseconds worth of scheduling. This should
  // suffice for most usage without having tasks scheduled for a later round.
  private[finagle] val nettyHwt = new netty.HashedWheelTimer(
    new NamedPoolThreadFactory("Finagle Default Timer", /*daemons = */true),
    TickDuration.inMilliseconds, TimeUnit.MILLISECONDS,
    TicksPerWheel)

  val Default: Timer = new HashedWheelTimer(nettyHwt)

  TimerStats.deviation(
    nettyHwt,
    10.milliseconds,
    FinagleStatsReceiver.scope("timer"))

  TimerStats.hashedWheelTimerInternals(
    nettyHwt,
    () => 10.seconds,
    FinagleStatsReceiver.scope("timer"))
}

/**
 * Retained for compatibility. Prefer [[HashedWheelTimer]].
 */
object DefaultTimer {
  private[finagle] val netty = HashedWheelTimer.nettyHwt

  val twitter: Timer = HashedWheelTimer.Default

  val get: DefaultTimer.type = this

  override def toString: String = "DefaultTimer"
}
