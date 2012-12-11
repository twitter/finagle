package com.twitter.finagle.util

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.util.{Time, Duration, Future, TimerTask, Timer}
import java.util.Collections
import java.util.concurrent.{TimeUnit, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.{util => nu}
import scala.collection.JavaConverters._

/**
 * A Finagle-managed timer, with interfaces for both
 * {{com.twitter.util.Timer}} and {{org.jboss.netty.util.Timer}}.
 * These are backed by the same underlying timer.
 */
trait FinagleTimer {
  val twitter: com.twitter.util.Timer
  val netty: org.jboss.netty.util.Timer

  /**
   * Dispose of the timer. This will cancel pending timeouts. It is
   * invalid to create new timeouts after a call to {{dispose()}}.
   */
  def dispose()
}

/**
 * The global {{SharedTimer}}, backed with a Netty {{HashedWheelTimer}}.
 */
private[finagle] object SharedTimer extends SharedTimer(
  () => new HashedWheelTimer(TimerThreadFactory, 10, TimeUnit.MILLISECONDS))
private[finagle] object TimerThreadFactory extends NamedPoolThreadFactory("Finagle Timer")

/**
 * Maintain a view on top of `underlying` with its own set of pending
 * timeouts. This timer is then independently stoppable.
 */
private[finagle] class NettyTimerView(underlying: nu.Timer) extends nu.Timer {
  private[this] val pending = Collections.newSetFromMap(
    new ConcurrentHashMap[nu.Timeout, java.lang.Boolean])
  private[this] val active = new AtomicBoolean(true)

  def newTimeout(task: nu.TimerTask, delay: Long, unit: TimeUnit) = {
    // This does not catch all misuses: there's a race between
    // calling `newTimeout` and `stop()`.
    require(active.get, "newTimeout on inactive timer")
    @volatile var ran = false
    val wrappedTask = new nu.TimerTask {
      def run(timeout: nu.Timeout) {
        ran = true
        pending.remove(timeout)
        task.run(timeout)
      }
    }
    val timeout = underlying.newTimeout(wrappedTask, delay, unit)
    pending.add(timeout)
    // Avoid a race where the task might be run before we even add it
    // to the pending set. If ran isn't set here we know that we
    // haven't attempted to remove the task yet. The remaining races
    // are fine since removal is idempotent.
    if (ran)
      pending.remove(timeout)
    new nu.Timeout {
      def getTimer() = timeout.getTimer()
      def getTask() = wrappedTask
      def isExpired() = timeout.isExpired()
      def isCancelled() = timeout.isCancelled()
      def cancel() {
        pending.remove(timeout)
        timeout.cancel()
      }
    }
  }

  def stop(): java.util.Set[nu.Timeout] = {
    require(active.getAndSet(false), "stop called twice")
    for (timeout <- pending.asScala)
      timeout.cancel()
    pending
  }
}

/**
 * Share an underlying timer with a managed lifecycle.
 */
private[finagle] class SharedTimer(mkTimer: () => nu.Timer) {
  private[this] sealed trait State
  private[this] object Quiet extends State
  private[this] case class Busy(timer: nu.Timer, count: Int) extends State

  @volatile private[this] var state: State = Quiet

  private[this] def wrap(underlying: nu.Timer) = new FinagleTimer {
    val netty = new NettyTimerView(underlying)
    val twitter = new TimerFromNettyTimer(netty)

    def dispose() {
      netty.stop()  // timer.stop() ensures it is called only once

      val stopTimer = SharedTimer.this.synchronized {
        state match {
          case Quiet =>
            assert(false, "state is Quiet, expected Busy")
            None
          case Busy(timer, 1) =>
            state = Quiet
            Some(timer)
          case Busy(timer, n) =>
            state = Busy(timer, n-1)
            None
        }
      }

      for (stopTimer <- stopTimer) {
        val thr = new Thread {
          override def run() {
            val pending = stopTimer.stop()
            // At this point, every timeout should have been cancelled.
            if (!pending.isEmpty) {
              java.util.logging.Logger.getLogger("").warning(
                "There were pending timeouts when timer stopped")
              for (timeout <- pending.asScala)
                timeout.cancel()
            }
          }
        }
        thr.start()
      }
    }
  }

  /**
   * Get a new {{FinagleTimer}}, creating the underlying timer if
   * necessary.
   */
  def acquire(): FinagleTimer = synchronized {
    state match {
      case Quiet =>
        val newTimer = mkTimer()
        state = Busy(newTimer, 1)
        wrap(newTimer)
      case Busy(timer, count) =>
        state = Busy(timer, count+1)
        wrap(timer)
    }
  }
}

/**
 * Implements a [[com.twitter.util.Timer]] in terms of a
 * [[org.jboss.netty.util.Timer]].
 */
class TimerFromNettyTimer(underlying: nu.Timer) extends Timer {
  def schedule(when: Time)(f: => Unit): TimerTask = {
    val timeout = underlying.newTimeout(new nu.TimerTask {
      def run(to: nu.Timeout) {
        if (!to.isCancelled) f
      }
    }, math.max(0, (when - Time.now).inMilliseconds), TimeUnit.MILLISECONDS)
    toTimerTask(timeout)
  }

  def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = new TimerTask {
    var isCancelled = false
    var ref: TimerTask = schedule(when) { loop() }

    def loop() {
      f
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

  def stop() { underlying.stop() }

  private[this] def toTimerTask(task: nu.Timeout) = new TimerTask {
    def cancel() { task.cancel() }
  }
}

object DefaultTimer extends HashedWheelTimer(
    new NamedPoolThreadFactory("Finagle Default Timer", true/*daemons*/),
    10, TimeUnit.MILLISECONDS
) {
  val twitter = new TimerFromNettyTimer(this)
}
