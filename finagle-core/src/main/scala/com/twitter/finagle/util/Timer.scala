package com.twitter.finagle.util

import collection.mutable.HashSet

import java.util.concurrent.{TimeUnit, Executors}
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger, AtomicBoolean}
import org.jboss.netty.util.{HashedWheelTimer}
import org.jboss.netty.{util => nu}

import com.twitter.util.{
  Time, Duration, TimerTask, ReferenceCountedTimer,
  ReferenceCountingTimer, ThreadStoppingTimer}
import com.twitter.concurrent.NamedPoolThreadFactory

import com.twitter.finagle.stats.{StatsReceiver, GlobalStatsReceiver}

private[finagle] object Timer {
  private[this] val timerStoppingExecutor = Executors.newFixedThreadPool(
    1, new NamedPoolThreadFactory("FINAGLE-TIMER-STOPPER", true/*daemon*/))

  def register(h: CloseNotifier) {
      default.acquire()
      h.onClose {
        default.stop()
      }
  }

  // This timer should only be used inside the context of finagle,
  // since it requires explicit reference count management. (Via the
  // builder routines.) We jump through a ridiculous number of hoops
  // here to make this work reliably:
  //
  //   Reference counting for resource management within finagle (so
  //   we can actually quit processes.)
  //
  //   `ThreadStoppingTimer` to ensure that timer threads themselves
  //   can shut down reliably (they cannot be shut down from the timer
  //   threads themselves.)
  implicit val default: ReferenceCountedTimer = {
    def factory() = {
      val underlying = new Timer(new HashedWheelTimer(10, TimeUnit.MILLISECONDS))
      new ThreadStoppingTimer(underlying, timerStoppingExecutor)
    }

    val underlying = new ReferenceCountingTimer(factory)
    new CountingTimer(underlying) with ReferenceCountedTimer {
      private[this] val gauge =
        GlobalStatsReceiver.addGauge("timeouts") { count }

      def acquire() = underlying.acquire()
    }
  }

  // Spin off a thread to close.
  val defaultNettyTimer = new TimerToNettyTimer(default)
}

/**
 * Implements the Netty timer interface on top of `default`, which is
 * reference counted. This allows us to provide a reference counted
 * (via cancellation) timer to Netty classes that need it. This may be
 * slightly confusing since `default` in turn is implemented on top of
 * a Netty timer.
 */
private[finagle] class TimerToNettyTimer(underlying: ReferenceCountedTimer)
  extends nu.Timer
{
  private[this] object State extends Enumeration {
    type State = Value
    val Pending, Expired, Cancelled = Value
  }

  def newTimeout(task: nu.TimerTask, delay: Long, unit: TimeUnit) = {
    @volatile var underlyingTask: TimerTask = null
    val timeout = new nu.Timeout {
      import State._
      private[this] val state = new AtomicReference[State](Pending)
      private[this] def transition(newState: State)(onSucc: => Unit) {
        if (state.compareAndSet(Pending, newState)) {
          onSucc
          underlying.stop()
        }
      }

      def isCancelled = state.get == Cancelled
      def getTask() = task
      def getTimer = TimerToNettyTimer.this
      def isExpired = state.get == Expired || isCancelled

      def cancel() {
        transition(Cancelled) {
          underlyingTask.cancel()
        }
      }

      def run() {
        transition(Expired) {
          task.run(this)
        }
      }
    }

    underlying.acquire()
    val when = Time.now + Duration.fromTimeUnit(delay, unit)
    underlyingTask = underlying.schedule(when) { timeout.run() }
    timeout
  }

  def stop() = {
    throw new Exception(
      "stop() has not been implemented for the wrapped netty timer.")
  }
}

/**
 * Implements a [[com.twitter.util.Timer]] in terms of a
 * [[org.jboss.netty.util.Timer]].
 */
class Timer(underlying: nu.Timer) extends com.twitter.util.Timer {
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

class CountingTimer(underlying: com.twitter.util.Timer)
  extends com.twitter.util.Timer
{
  private[this] val npending = new AtomicInteger(0)

  def count = npending.get

  private[this] def wrap(underlying: (=> Unit) => TimerTask, f: => Unit) = {
    val decr = new AtomicBoolean(false)
    npending.incrementAndGet()

    val underlyingTask = underlying {
      if (!decr.getAndSet(true))
        npending.decrementAndGet()
      f
    }

    new TimerTask {
      def cancel() {
        if (!decr.getAndSet(true))
          npending.decrementAndGet()
        underlyingTask.cancel()
      }
    }
  }

  def schedule(when: Time)(f: => Unit): TimerTask =
    wrap(underlying.schedule(when), f)

  override def schedule(when: Time, period: Duration)(f: => Unit): TimerTask =
    wrap(underlying.schedule(when, period), f)

  def stop() { underlying.stop() }
}
