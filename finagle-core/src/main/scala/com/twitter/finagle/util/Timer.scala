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
 * A Netty timer convertible to a com.twitter.util.Timer
 */
private[finagle] trait TwoTimer extends nu.Timer {
  val toTwitterTimer: Timer
}

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
    val wrappedTask = new nu.TimerTask {
      def run(timeout: nu.Timeout) {
        pending.remove(timeout)
        task.run(timeout)
      }
    }
    val timeout = underlying.newTimeout(wrappedTask, delay, unit)
    pending.add(timeout)
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
 * A Managed version of a TwoTimer. Shares an underlying timer and
 * multiplexes several views onto it. Each view may in turn be
 * discarded only once. For safety, a new thread is spun up in order
 * to stop the timer, since Netty's HashedWheelTimer disallows
 * stopping inside of a timer thread.
 */
private[finagle] class ManagedNettyTimer(mkTimer: () => nu.Timer) extends Managed[TwoTimer] {
  private[this] sealed trait State
  private[this] object Quiet extends State
  private[this] case class Busy(timer: nu.Timer, count: Int) extends State

  @volatile private[this] var state: State = Quiet

  private[this] def wrap(underlying: nu.Timer) = new Disposable[TwoTimer] {
    private[this] val timer = new NettyTimerView(underlying) with TwoTimer {
      val toTwitterTimer = new TimerFromNettyTimer(this)
    }

    def get = timer

    def dispose(deadline: Time) = {
      timer.stop()  // requires(active)

      val stopTimer = ManagedNettyTimer.this.synchronized {
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

      stopTimer foreach { timer =>
        val thr = new Thread {
          override def run() {
            val pending = timer.stop()
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

      Future.Done
    }
  }

  def make(): Disposable[TwoTimer] = synchronized {
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

  val toTwitterTimer = map(new TimerFromNettyTimer(_))
}

private[finagle] object TimerThreadFactory extends NamedPoolThreadFactory("Finagle Timer")
private[finagle] object ManagedTimer extends ManagedNettyTimer(
  () => new HashedWheelTimer(TimerThreadFactory, 10, TimeUnit.MILLISECONDS))

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
    }, (when - Time.now).inMilliseconds max 0, TimeUnit.MILLISECONDS)
    toTimerTask(timeout)
  }

  def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = {
    schedule(when) {
      f
      schedule(period)(f)
    }
  }

  def stop() { underlying.stop() }

  private[this] def toTimerTask(task: nu.Timeout) = new TimerTask {
    def cancel() { task.cancel() }
  }
}

