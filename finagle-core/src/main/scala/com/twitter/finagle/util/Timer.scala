package com.twitter.finagle.util

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.conversions.time._
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.util.{Timer => UtilTimer, _}
import java.util.concurrent.TimeUnit
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.{util => nu}

/**
 * Implements a [[com.twitter.util.Timer]] in terms of a
 * [[org.jboss.netty.util.Timer]].
 */
class TimerFromNettyTimer(underlying: nu.Timer) extends UtilTimer {
  def schedule(when: Time)(f: => Unit): TimerTask = {
    val timeout = underlying.newTimeout(new nu.TimerTask {
      val saved = Local.save()
      def run(to: nu.Timeout) {
        if (!to.isCancelled) runInContext(saved, f)
      }
    }, math.max(0, (when - Time.now).inMilliseconds), TimeUnit.MILLISECONDS)
    toTimerTask(timeout)
  }

  def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = new TimerTask {
    val saved = Local.save()
    var isCancelled = false
    var ref: TimerTask = schedule(when) { loop() }

    def loop() {
      runInContext(saved, f)
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

  private[this] def runInContext(saved: Local.Context, f: => Unit): Unit = {
    Local.restore(saved)
    Monitor(f)
  }
}

// Note: this uses the default `ticksPerWheel` size of 512 and 10 millisecond ticks,
// which gives ~5100 milliseconds worth of scheduling. This should suffice
// for most usage without having tasks scheduled for a later round.
object DefaultTimer extends HashedWheelTimer(
    new NamedPoolThreadFactory("Finagle Default Timer", true/*daemons*/),
    10, TimeUnit.MILLISECONDS)
{
  val twitter = new TimerFromNettyTimer(this)

  override def toString: String = "DefaultTimer"

  val get: DefaultTimer.type = this

  TimerStats.deviation(
    this,
    10.milliseconds,
    FinagleStatsReceiver.scope("timer"))

  TimerStats.hashedWheelTimerInternals(
    this,
    () => 10.seconds,
    FinagleStatsReceiver.scope("timer"))
}
