package com.twitter.finagle

import collection.mutable.ArrayBuffer

import com.twitter.util.{Time, Duration}

class MockTimer extends com.twitter.util.Timer {
  case class Task(var when: Time, runner: Function0[Unit])
    extends com.twitter.util.TimerTask
  {
    var isCancelled = false
    def cancel() { isCancelled = true; when = Time.now; tick() }
  }

  var isStopped = false
  var tasks = ArrayBuffer[Task]()

  def tick() {
    if (isStopped)
      throw new Exception("timer is stopped already")

    val now = Time.now
    val (toRun, toQueue) = tasks.partition { task => task.when <= now }
    tasks = toQueue
    toRun filter { !_.isCancelled } foreach { _.runner() }
  }

  def schedule(when: Time)(f: => Unit) = {
    val task = Task(when, () => f)
    tasks += task
    task
  }

  def schedule(when: Time, period: Duration)(f: => Unit) =
    throw new Exception("periodic scheduling not supported")

  def stop() { isStopped = true }
}
