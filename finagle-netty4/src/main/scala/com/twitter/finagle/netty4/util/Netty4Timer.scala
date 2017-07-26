package com.twitter.finagle.netty4.util

import com.twitter.util.{Duration, Time, Timer, TimerTask}
import java.util.concurrent.TimeUnit

/**
 * An implementation of Finagle's [[Timer]] based on Netty 4 [[io.netty.util.Timer]].
 */
private[netty4] class Netty4Timer(underlying: io.netty.util.Timer) extends Timer {

  protected def scheduleOnce(when: Time)(f: => Unit): TimerTask =
    new TimerTask {
      private[this] val timeout = underlying.newTimeout(
        new io.netty.util.TimerTask {
          def run(timeout: io.netty.util.Timeout): Unit = if (!timeout.isCancelled) { f }
        },
        math.max(0, (when - Time.now).inMilliseconds),
        TimeUnit.MILLISECONDS
      )

      def cancel(): Unit = timeout.cancel()
    }

  protected def schedulePeriodically(when: Time, period: Duration)(f: => Unit): TimerTask =
    new TimerTask {
      // The thread-safety is guaranteed by synchronizing on `this`.
      private[this] var cancelled = false
      private[this] var task = schedule(when) { loop() }

      private[this] final def loop(): Unit = synchronized {
        if (!cancelled) {
          f
          task = schedule(period.fromNow) { loop() }
        }
      }

      def cancel(): Unit = synchronized {
        cancelled = true
        task.cancel()
      }
    }

  def stop(): Unit = underlying.stop()

  override def toString: String = "Netty4Timer"
}
