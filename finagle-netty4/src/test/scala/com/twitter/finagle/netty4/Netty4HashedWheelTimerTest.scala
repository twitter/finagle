package com.twitter.finagle.netty4

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{CountDownLatch, Time}
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

class Netty4HashedWheelTimerTest extends FunSuite
  with Eventually
  with IntegrationPatience {

  test("slow tasks are observed") {
    val stats = new InMemoryStatsReceiver()
    val maxRuntime = 20.milliseconds
    val timerThreads = new TimerThreads(
      stats,
      maxRuntime = maxRuntime,
      maxLogFrequency = 0.milliseconds)
    val hashedWheelTimer = new HashedWheelTimer(stats, timerThreads)
    val timer = new Netty4HashedWheelTimer(hashedWheelTimer)

    val slowIsStarted = new CountDownLatch(1)
    val letSlowGo = new CountDownLatch(1)
    // schedule a task that will be delayed for right now
    timer.schedule(Time.now + 1.millisecond) {
      // take a "nap" to trigger the threshold
      Thread.sleep((maxRuntime + 100.millis).inMillis)
      // let the main thread know its safe to queue work
      slowIsStarted.countDown()
      // then wait for the other work to finish its checks
      // such that we are free to go.
      assert(letSlowGo.await(25.seconds))
    }

    // wait for the slow task to start before scheduling more work
    // to trigger the checks
    assert(slowIsStarted.await(25.seconds))
    timer.schedule(Time.now + 35.milliseconds) {
      // the first task is still running, this work is blocked until it finishes
      ()
    }
    // release the latch to unblock our slow task and the other
    letSlowGo.countDown()

    // verify the metrics got updated
    eventually {
      assert(1 == stats.counter("timer", "slow")())
    }
  }

}
