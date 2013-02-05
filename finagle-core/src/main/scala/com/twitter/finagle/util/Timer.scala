package com.twitter.finagle.util

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.util.{Time, Duration, TimerTask, Timer}
import java.util.Collections
import java.util.concurrent.TimeUnit
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.{util => nu}

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
