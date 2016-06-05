package com.twitter.finagle.util

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.conversions.time._
import com.twitter.finagle.stats.{ReadableStat, InMemoryStatsReceiver}
import java.util.concurrent.TimeUnit
import org.jboss.netty.{util => netty}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class TimerStatsTest extends FunSuite
  with MockitoSugar
  with Eventually
  with IntegrationPatience
{

  test("deviation") {
    val tickDuration = 10.milliseconds
    val hwt = new netty.HashedWheelTimer(
      new NamedPoolThreadFactory(getClass.getSimpleName),
      tickDuration.inMillis, TimeUnit.MILLISECONDS)
    val sr = new InMemoryStatsReceiver()
    val deviation: ReadableStat = sr.stat("deviation_ms")
    assert(deviation().isEmpty)

    TimerStats.deviation(hwt, tickDuration, sr)

    // assert that we capture at least 3 samples
    eventually {
      assert(deviation().size >= 3)
    }
    hwt.stop()
  }

  test("hashedWheelTimerInternals") {
    val tickDuration = 10.milliseconds
    val hwt = new netty.HashedWheelTimer(
      new NamedPoolThreadFactory(getClass.getSimpleName),
      tickDuration.inMillis, TimeUnit.MILLISECONDS)
    val sr = new InMemoryStatsReceiver()
    val pendingTimeouts: ReadableStat = sr.stat("pending_tasks")

    // nothing should be scheduled at first
    assert(pendingTimeouts().isEmpty)

    val nTasks = 5
    // schedule some tasks, but they won't run for 10 minutes
    // to ensure they are queued up when the monitoring task runs
    for (_ <- 0.until(nTasks))
      hwt.newTimeout(mock[netty.TimerTask], 10, TimeUnit.MINUTES)

    // kick off the task to do the monitoring.
    // have the monitoring task to run quickly the first time and only once.
    var count = 0
    val nextRunAt = () => {
      count += 1
      if (count == 1) 1.millisecond else 5.minutes
    }
    TimerStats.hashedWheelTimerInternals(hwt, nextRunAt, sr)

    eventually {
      // we should have the nTasks tasks pending that we've scheduled
      // plus it should see the monitoring task itself which runs it.
      assert(pendingTimeouts() == Seq(nTasks + 1))
    }

    hwt.stop()
  }

}
