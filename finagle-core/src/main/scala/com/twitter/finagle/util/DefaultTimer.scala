package com.twitter.finagle.util

import com.twitter.logging.Logger
import com.twitter.util.{JavaTimer, ProxyTimer, Timer}

/**
 * A Finagle's trusty timer that should satisfy a certain level of throughput/latency
 * requirements: O(1) task creation and (at least) O(log n) task cancellation. Usually,
 * hashed wheel timers provide a great foundation fulfilling those requirements.
 *
 * @note This is package-private such that we have a control over which timer implementations
 *       might be considered "default" for Finagle.
 */
private[finagle] abstract class DefaultTimer extends Timer

/**
 * Finagle's default [[Timer]] that's intended to be shared across a number of servers/clients.
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
  protected val self: Timer = LoadService[DefaultTimer]() match {
    case loaded +: _ => loaded
    case _ =>
      log.warning(s"Can not service-load a timer. Using JavaTimer instead.")
      new JavaTimer(isDaemon = true)
  }

  /**
   * An alias for [[DefaultTimer]].
   */
  @deprecated("Use DefaultTimer from Scala and DefaultTimer.getInstance() from Java", "2017-5-4")
  val twitter: Timer = this

  /**
   * An alias for [[DefaultTimer]].
   */
  @deprecated("Use DefaultTimer from Scala and DefaultTimer.getInstance() from Java", "2017-5-4")
  val get: DefaultTimer.type = this

  /**
   * A Java-friendly accessor for [[DefaultTimer]].
   */
  def getInstance: Timer = this

  override def stop(): Unit =
    log.warning(s"Ignoring call to `Timer.stop()` on an unstoppable DefaultTimer.\n" +
      s"Current stack trace: ${ Thread.currentThread.getStackTrace.mkString("\n") }")

  override def toString: String = s"DefaultTimer(${self.toString})"
}
