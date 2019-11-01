package com.twitter.finagle.serverset2

import com.twitter.conversions.DurationOps._
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.stats.{FinagleStatsReceiver, Stat, Verbosity}
import com.twitter.util._

/**
 * An Epoch is an Event that notifies its listener once per `period`
 */
private[serverset2] class Epoch(val event: Event[Unit], val period: Duration)

private[serverset2] object Epoch {
  private val epochTimer: Timer = new ScheduledThreadPoolTimer(
    poolSize = 1,
    new NamedPoolThreadFactory("finagle-serversets Stabilizer timer", /*makeDaemons = */ true)
  )

  private val notifyMs: Stat = FinagleStatsReceiver
    .stat(Verbosity.Debug, "serverset2", "stabilizer", "notify_ms")

  /**
   * Create an event of epochs for the given duration.
   */
  def apply(period: Duration, timer: Timer = epochTimer): Epoch =
    new Epoch(
      new Event[Unit] {
        // accommodate Timers that have a minimum floor.
        private[this] val schedulingPeriod = period.max(1.millisecond)

        def register(w: Witness[Unit]): Closable = {
          timer.schedule(schedulingPeriod) {
            val elapsed = Stopwatch.start()
            w.notify(())
            notifyMs.add(elapsed().inMilliseconds)
          }
        }
      },
      period
    )
}
