package com.twitter.finagle.serverset2.client

import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.util.{Closable, Duration, Future, Time, Timer}

class StateTracker(statsReceiver: StatsReceiver, samplePeriod: Duration, timer: Timer)
    extends Closable {

  private[this] var currCounter: Option[Counter] = None
  private[this] var lastSample: Time = Time.now

  private[this] val timerTask = timer.schedule(Time.now + samplePeriod, samplePeriod) {
    sample()
  }

  def close(deadline: Time): Future[Unit] = {
    timerTask.close(deadline)
  }

  def transition(newState: SessionState): Unit = synchronized {
    sample()
    currCounter = Some(statsReceiver.counter(s"${newState.name}_duration_ms"))
  }

  private[this] def sample(): Unit = synchronized {
    val now = Time.now
    val delta = now - lastSample
    lastSample = now
    currCounter.foreach { counter => counter.incr(delta.inMilliseconds.toInt) }
  }
}
