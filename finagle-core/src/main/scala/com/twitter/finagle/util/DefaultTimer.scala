package com.twitter.finagle.util

import com.twitter.app.GlobalFlag
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.JavaTimer
import com.twitter.util.ProxyTimer
import com.twitter.util.Timer

/**
 * Configures whether to probe for slow tasks executing in the default `Timer`.
 *
 * When enabled, tasks are monitored to detect tasks that are slow to complete. A counter of
 * the number of slow tasks is registered at `finagle/timer/slow`. Additionally, if a slow
 * task is observed executing the stack traces of all threads will be logged at level
 * WARN. The maximum runtime and minimum interval between logging stack traces can be tuned
 * using the global flags `c.t.f.u.defaultTimerSlowTaskMaxRuntime` and
 * `c.t.f.u.defaultTimerSlowTaskLogMinInterval`, defined below.
 *
 * @note Observation of a slow task in progress is performed when scheduling additional work
 *       and is thus susceptible to false negatives.
 */
object defaultTimerProbeSlowTasks
    extends GlobalFlag(false, "Enable reporting of slow timer tasks executing in the default timer")

/**
 * Configures the maximum allowed runtime for tasks executing in the default `Timer`.
 */
object defaultTimerSlowTaskMaxRuntime
    extends GlobalFlag(2.seconds, "Maximum runtime allowed for tasks before they are reported")

/**
 * Configures the minimum duration between logging stack traces when a slow task is
 * detected in the default `Timer`.
 */
object defaultTimerSlowTaskLogMinInterval
    extends GlobalFlag(20.seconds, "Minimum interval between recording stack traces for slow tasks")

/**
 * A Finagle's trusty timer that should satisfy a certain level of throughput/latency
 * requirements: O(1) task creation and (at least) O(log n) task cancellation. Usually,
 * hashed wheel timers provide a great foundation fulfilling those requirements.
 *
 * @note This is package-private such that we have a control over which timer implementations
 *       might be considered "default" for Finagle.
 */
private[finagle] trait ServiceLoadedTimer extends Timer

/**
 * Finagle's default [[Timer]] that's intended to be shared across a number of servers/clients.
 *
 * The default [[Timer]] is intended for scheduling tasks that will finish very quickly and
 * shouldn't be used to schedule tasks that will occupy the executing thread for a significant
 * duration.
 *
 * Use `DefaultTimer.Implicit` to import an implicit instance of this timer into the scope.
 *
 * {{{
 *  scala> import com.twitter.util.Future, com.twitter.conversions.DurationOps._
 *  scala> import com.twitter.finagle.util.DefaultTimer.Implicit
 *
 *  scala> val f = Future.sleep(10.seconds)
 *  f: com.twitter.util.Future[Unit] = <function0>
 * }}}
 *
 * Java users may prefer `DefaultTimer.getInstance()` to access this timer.
 *
 * @note This timer is "unstoppable" such that calls to `stop()` is ignored.
 */
object DefaultTimer extends ProxyTimer {

  private[this] val log = Logger.get()

  // This timer could be one of the following (in the order of priority):
  //
  // - loaded at runtime via the `LoadService` machinery (the first available timer is used)
  // - `JavaTimer`
  //
  // TODO: We might consider doing round-robin over the set of "default" timers in future.
  @volatile
  protected var _self: Timer = {
    val baseTimer = LoadService[ServiceLoadedTimer]() match {
      case loaded +: _ => loaded
      case _ =>
        log.warning(s"Can not service-load a timer. Using JavaTimer instead.")
        new JavaTimer(isDaemon = true)
    }
    initializeDefaultTimer(baseTimer)
  }

  protected def self: Timer = _self

  /**
   * Set the implementation of the underlying Timer.
   * Use with caution for changing DefaultTimer after a Timer has
   * been service loaded at runtime.
   *
   * @note This can be unsafe if the Timer is slow and fails to
   * meet throughput/latency requirements for task creation and
   * task cancelation.
   */
  def setUnsafe(timer: Timer): Unit = { _self = timer }

  /**
   * An implicit instance supplied for use in the Future.* methods.
   */
  implicit val Implicit: Timer = this

  /**
   * An alias for [[DefaultTimer]].
   */
  @deprecated("Use DefaultTimer from Scala and DefaultTimer.getInstance() from Java", "2017-5-4")
  val twitter: Timer = this

  /**
   * A Java-friendly accessor for [[DefaultTimer]].
   */
  def getInstance: Timer = this

  override def stop(): Unit =
    log.warning(
      s"Ignoring call to `Timer.stop()` on an unstoppable DefaultTimer.\n" +
        s"Current stack trace: ${Thread.currentThread.getStackTrace.mkString("\n")}"
    )

  override def toString: String = s"DefaultTimer(${_self.toString})"

  private[this] def initializeDefaultTimer(timer: Timer): Timer = {
    if (!defaultTimerProbeSlowTasks()) timer
    else {
      // Probing for slow running tasks is enabled so wrap the timer in the metering proxy.
      new LoggingSlowProbeProxyTimer(
        underlying = timer,
        FinagleStatsReceiver,
        maxRuntime = defaultTimerSlowTaskMaxRuntime(),
        maxLogFrequency = defaultTimerSlowTaskLogMinInterval()
      )
    }
  }
}
