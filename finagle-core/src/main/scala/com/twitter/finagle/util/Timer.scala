package com.twitter.finagle.util

import collection.mutable.HashSet

import org.jboss.netty.util.{HashedWheelTimer, Timeout}

import java.util.concurrent.{TimeUnit, Executors}
import java.util.concurrent.atomic.AtomicReference
import com.twitter.util.{
  Time, Duration, TimerTask,
  ReferenceCountedTimer, ThreadStoppingTimer}
import com.twitter.concurrent.NamedPoolThreadFactory

private[finagle] object Timer {
  private[this] val timerStoppingExecutor = Executors.newFixedThreadPool(
    1, new NamedPoolThreadFactory("FINAGLE-TIMER-STOPPER", true/*daemon*/))

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
  implicit val default = {
    def factory() = {
      val underlying = new Timer(new HashedWheelTimer(10, TimeUnit.MILLISECONDS))
      new ThreadStoppingTimer(underlying, timerStoppingExecutor)
    }

    new ReferenceCountedTimer(factory)
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
  extends org.jboss.netty.util.Timer
{
  private[this] object State extends Enumeration {
    type State = Value
    val Pending, Expired, Cancelled = Value
  }

  def newTimeout(task: org.jboss.netty.util.TimerTask, delay: Long, unit: TimeUnit) = {
    val timeout = new Timeout {
      import State._
      private[this] val state = new AtomicReference[State](Pending)
      private[this] def transition(targetState: State) {
        if (state.compareAndSet(Pending, targetState)) {
          if (targetState == Expired)
            task.run(this)
          underlying.stop()
        }
      }

      def cancel()    = transition(Cancelled)
      def isCancelled = state.get == Cancelled
      def getTask()   = task
      def getTimer    = TimerToNettyTimer.this
      def isExpired   = state.get == Expired || isCancelled
      def run()       = transition(Expired)
    }

    underlying.acquire()
    underlying.schedule(Time.now + Duration.fromTimeUnit(delay, unit)) {
      timeout.run()
    }

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
class Timer(underlying: org.jboss.netty.util.Timer) extends com.twitter.util.Timer {
  def schedule(when: Time)(f: => Unit): TimerTask = {
    val timeout = underlying.newTimeout(new org.jboss.netty.util.TimerTask {
      def run(to: Timeout) {
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

  private[this] def toTimerTask(task: Timeout) = new TimerTask {
    def cancel() { task.cancel() }
  }
}
