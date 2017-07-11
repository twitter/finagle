package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.conversions.time._
import com.twitter.finagle.netty4.util.Netty4Timer
import com.twitter.finagle.stats.{FinagleStatsReceiver, StatsReceiver}
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Time, TimerTask}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

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
 * This makes assumptions that the Netty timer will only have a single thread
 * running tasks and there is a good amount of work being scheduled.
 * The former is the case with Netty's HashedWheelTimer.
 * The latter is done to avoid creating a dedicated thread for the checks.
 */
private class TimerThreads(
    statsReceiver: StatsReceiver,
    maxRuntime: Duration = 2.seconds,
    maxLogFrequency: Duration = 20.seconds) {

  private[this] val log = Logger.get()
  private[this] val slow = statsReceiver.counter("timer", "slow")

  @volatile
  private[this] var lastStartAt = Time.Top
  // thread-safety provided by synchronization on `this`
  private[this] var lastLog = Time.Bottom

  def taskStarted(): Unit = {
    lastStartAt = Time.now
  }

  def clearStart(): Unit = {
    lastStartAt = Time.Top
  }

  def checkSlowTask(): Unit = {
    val elapsed = Time.now - lastStartAt
    if (elapsed > maxRuntime) {
      slow.incr()
      // note that logging is done outside of the sync block.
      val doLog = synchronized {
        if (Time.now - lastLog > maxLogFrequency) {
          lastLog = Time.now
          true
        } else {
          false
        }
      }
      if (doLog) {
        log.warning(s"Timer task has been running for more than $maxRuntime ($elapsed), " +
          "current stacktraces follow.")
        Thread.getAllStackTraces.asScala.foreach { case (thread, stack) =>
          log.warning("Slow Timer task thread dump. Thread id=" +
            s"${thread.getId} '${thread.getName}': ${stack.mkString("\n\t", "\n\t", "")}")
        }
      }
    }
  }
}

/**
 * A default instance of Netty timer that needs to be shared between [[Netty4HashedWheelTimer]]
 * object (used in protocols) and instances (service-loaded).
 */
private class HashedWheelTimer(
    statsReceiver: StatsReceiver,
    val timerThreads: TimerThreads)
  extends io.netty.util.HashedWheelTimer(
    new NamedPoolThreadFactory("Netty 4 Timer", /*daemon = */true),
    timerTickDuration().inMilliseconds, TimeUnit.MILLISECONDS,
    timerTicksPerWheel(),
    /*leakDetection = */false) { self =>

  private[this] val statsPollInterval = 10.seconds

  private object deviationStat extends io.netty.util.TimerTask {

    private[this] val tickDuration = timerTickDuration()
    private[this] val deviationMs = statsReceiver.stat("timer", "deviation_ms")
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
    private[this] val pendingTasks = statsReceiver.stat("timer", "pending_tasks")

    def run(timeout: io.netty.util.Timeout): Unit = {
      pendingTasks.add(self.pendingTimeouts)
      self.newTimeout(pendingTasksStat, statsPollInterval.inSeconds, TimeUnit.SECONDS)
    }
  }

  self.newTimeout(deviationStat, timerTickDuration().inMilliseconds, TimeUnit.MILLISECONDS)
  self.newTimeout(pendingTasksStat, statsPollInterval.inSeconds, TimeUnit.SECONDS)
}

private object HashedWheelTimer {
  /**
   * A singleton instance of [[HashedWheelTimer]] reporting stats into
   * the "finagle" scope.
   */
  val instance: HashedWheelTimer =
    new HashedWheelTimer(
      FinagleStatsReceiver,
      new TimerThreads(
        FinagleStatsReceiver, maxRuntime = 2.seconds, maxLogFrequency = 20.seconds))
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
 * 3. `slow`
 *
 * To configure this timer use the following CLI flags:
 *
 * 1. `-com.twitter.finagle.netty4.timerTickDuration=100.milliseconds`
 * 2. `-com.twitter.finagle.netty4.timerTicksPerWheel=512`
 */
private[netty4] class Netty4HashedWheelTimer(hashedWheelTimer: HashedWheelTimer)
  extends Netty4Timer(hashedWheelTimer) {

  def this() = this(HashedWheelTimer.instance)

  private[this] val log = Logger.get()

  // This timer is "unstoppable".
  override def stop(): Unit =
    log.warning(s"Ignoring call to `Timer.stop()` on an unstoppable Netty4Timer.\n" +
      s"Current stack trace: ${Thread.currentThread.getStackTrace.mkString("\n")}")

  override protected def scheduleOnce(when: Time)(f: => Unit): TimerTask = {
    // let another thread check if the timer thread has been slow.
    // while this could be the timer thread scheduling more work,
    // we expect that at some point another thread will schedule something.
    // while this relies on application's doing scheduling to trigger
    // the findings, we expect that to be common. the alternative would've
    // been to use a separate dedicated thread, but the cost didn't seem
    // worth the benefits to me.
    hashedWheelTimer.timerThreads.checkSlowTask()
    super.scheduleOnce(when) {
      // mark this task as started, then finished in a finally block.
      hashedWheelTimer.timerThreads.taskStarted()
      try f finally hashedWheelTimer.timerThreads.clearStart()
    }
  }

  override protected def schedulePeriodically(
    when: Time,
    period: Duration
  )(
    f: => Unit
  ): TimerTask = {
    // see comments in `scheduleOnce` for explanations.
    hashedWheelTimer.timerThreads.checkSlowTask()
    super.schedulePeriodically(when, period) {
      hashedWheelTimer.timerThreads.taskStarted()
      try f finally hashedWheelTimer.timerThreads.clearStart()
    }
  }
}

/**
 * A singleton instance of [[Netty4HashedWheelTimer]].
 */
private[finagle] object Netty4HashedWheelTimer extends Netty4HashedWheelTimer
