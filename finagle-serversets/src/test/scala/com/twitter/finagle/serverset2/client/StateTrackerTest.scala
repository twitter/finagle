package com.twitter.finagle.serverset2.client

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{MockTimer, Time}
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StateTrackerTest extends FlatSpec {
  val statsReceiver = new InMemoryStatsReceiver

  "StateTracker" should "correctly count state durations" in {
    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer
      val stateTracker = new StateTracker(statsReceiver, 1.second, timer)
      stateTracker.transition(SessionState.SyncConnected)
      tc.advance(2000.milliseconds)
      timer.tick() // StateTracker.timerTask should fire, increment
      tc.advance(250.milliseconds)
      timer.tick() // StateTracker.timerTask should NOT fire
      assert(statsReceiver.counter(s"${SessionState.SyncConnected.name}_duration_ms")() == 2000)
      stateTracker.transition(SessionState.Disconnected) // StateTracker.update should run
      assert(statsReceiver.counter(s"${SessionState.SyncConnected.name}_duration_ms")() == 2250)
      tc.advance(500.milliseconds)
      timer.tick() // StateTracker.timerTask should NOT fire
      assert(statsReceiver.counter(s"${SessionState.Disconnected.name}_duration_ms")() == 0)
      stateTracker.transition(SessionState.SyncConnected) // StateTracker.update should run
      assert(statsReceiver.counter(s"${SessionState.Disconnected.name}_duration_ms")() == 500)
      tc.advance(5000.milliseconds)
      timer.tick() // StateTracker.timerTask should fire, increment
      assert(statsReceiver.counter(s"${SessionState.SyncConnected.name}_duration_ms")() == 7250)
    }
  }
}
