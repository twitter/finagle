package com.twitter.finagle.util

import java.util.concurrent.TimeUnit
import com.twitter.util.{Time, Duration, TimerTask, ReferenceCountedTimer}
import org.jboss.netty.util.{HashedWheelTimer, Timeout}

object Timer {
  // This timer should only be used inside the context of finagle,
  // since it requires explicit reference count management. (Via the
  // builder routines.)
  implicit val default =
    new ReferenceCountedTimer(() =>
      new Timer(new HashedWheelTimer(10, TimeUnit.MILLISECONDS)))
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
