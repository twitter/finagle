package com.twitter.finagle.util

import collection.mutable.HashSet

import java.util.concurrent.TimeUnit
import com.twitter.util.{Time, Duration, TimerTask, ReferenceCountedTimer}
import org.jboss.netty.util.{HashedWheelTimer, Timeout}

private[finagle] object Timer {
  // This timer should only be used inside the context of finagle,
  // since it requires explicit reference count management. (Via the
  // builder routines.)
  implicit val default =
    new ReferenceCountedTimer(() =>
      new Timer(new HashedWheelTimer(10, TimeUnit.MILLISECONDS)))

  val defaultNettyTimer = new org.jboss.netty.util.Timer {
    private[this] val theTimer = this
    private[this] val timeouts = new HashSet[Timeout]

    def newTimeout(task: org.jboss.netty.util.TimerTask, delay: Long, unit: TimeUnit) = {
      val timeout = new Timeout {
        private[this] var _isCancelled = false
        private[this] var _isExpired = false

        theTimer.synchronized { timeouts += this }

        def cancel() = synchronized { _isCancelled = true; run() }
        def isCancelled = synchronized { _isCancelled }

        def getTask() = task
        def getTimer = theTimer

        def isExpired = _isExpired

        def run(): Unit = synchronized {
          if (_isExpired)
            return

          theTimer.synchronized { timeouts -= this }
          _isExpired = true
          if (!isCancelled)
            task.run(this)
          default.stop()
        }
      }

      default.acquire()
      default.schedule(Time.now + Duration.fromTimeUnit(delay, unit)) {
        timeout.run()
      }

      timeout
    }

    def stop() = {
      throw new Exception(
        "stop() has not been implemented for the wrapped netty timer.")
    }
  }
}

class Timer(underlying: org.jboss.netty.util.Timer) extends com.twitter.util.Timer
{
  def schedule(when: Time)(f: => Unit): TimerTask = {
    val timeout = underlying.newTimeout(new org.jboss.netty.util.TimerTask {
      def run(to: Timeout) {
        if (!to.isCancelled) f
      }
    }, (when - Time.now).inMilliseconds, TimeUnit.MILLISECONDS)
    toTimerTask(timeout)
  }

  def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = {
    val task = schedule(when) {
      f
      schedule(period)(f)
    }
    task
  }

  def stop() { underlying.stop() }

  private[this] def toTimerTask(task: Timeout) = new TimerTask {
    def cancel() { task.cancel() }
  }
}

