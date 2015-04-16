package com.twitter.finagle.serverset2.client

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Closable, Duration, Future, Time, Timer}

class StateTracker(
  statsReceiver: StatsReceiver,
  samplePeriod: Duration,
  timer: Timer
) extends Closable {

  private[this] var currState: Option[SessionState] = None
  private[this] var lastSample: Time = Time.now

  private[this] val timerTask = timer.schedule(Time.now + samplePeriod, samplePeriod) {
    sample()
  }

  def close(deadline: Time): Future[Unit] = {
    timerTask.close(deadline)
  }

  def transition(newState: SessionState): Unit = synchronized {
    sample()
    currState = Some(newState)
  }

  private[this] def sample(): Unit = synchronized {
    val now = Time.now
    val delta = now - lastSample
    lastSample = now
    currState foreach { state =>
      statsReceiver.counter(s"${state.name}_duration_ms").incr(delta.inMilliseconds.toInt)
    }
  }
}
