package com.twitter.finagle.benchmark

import com.google.caliper.{SimpleBenchmark, Runner}
import com.twitter.conversions.time._
import com.twitter.finagle.util.{CountingTimer, TaskTrackingTimer}
import com.twitter.util.{Duration, Time, Timer, TimerTask}

// Run thus:
// bin/caliper finagle/finagle-benchmark com.twitter.finagle.benchmark.TaskTrackingTimerBenchmark

class FakeTask extends TimerTask {
  def cancel() {}
}

class FakeTimer extends Timer {
  def schedule(when: Time)(f: => Unit): TimerTask = new FakeTask
  def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = new FakeTask
  def stop() {}
}

class TaskTrackingTimerBenchmark extends SimpleBenchmark {
  private[this] val ntasks = 100

  // times adding and removing timer tasks to measure overhead of collection used
  def timeTaskAddAndRemove(nreps: Int) {
    val timer = new TaskTrackingTimer(new FakeTimer)
    val tasks = new Array[TimerTask](ntasks)

    var i, j = 0
    while (i < nreps) {
      while (j < ntasks) {
        tasks(j) = timer.schedule(1.seconds) {}
        j += 1
      }
      j = 0
      while (j < ntasks) {
        tasks(j).cancel()
        j += 1
      }
      i += 1
    }
    timer.stop()
  }

  // times different timer that doesn't track tasks for comparison
  def timeCountingTimer(nreps: Int) {
    val timer = new CountingTimer(new FakeTimer)
    val tasks = new Array[TimerTask](ntasks)

    var i, j = 0
    while (i < nreps) {
      while (j < ntasks) {
        tasks(j) = timer.schedule(1.seconds) {}
        j += 1
      }
      j = 0
      while (j < ntasks) {
        tasks(j).cancel()
        j += 1
      }
      i += 1
    }
    timer.stop()
  }
}