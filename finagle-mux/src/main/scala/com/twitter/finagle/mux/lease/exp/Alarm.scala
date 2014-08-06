package com.twitter.finagle.mux.lease.exp

import com.twitter.util.{Duration, Stopwatch, StorageUnit, Time}
import com.twitter.conversions.time.longToTimeableNumber
import com.twitter.conversions.storage.intToStorageUnitableWholeNumber

/**
 * `Alarm` describes whether a thread is ready to wake up, and if it's not
 * ready, how much longer it should sleep before checking again whether it's
 * ready to wake up or not.
 */
private[lease] trait Alarm {
  def sleeptime: Duration

  def finished: Boolean

  def min(other: Alarm): Alarm = new MinAlarm(this, other)
}

/**
 * The Alarm object is for running the alarm, which will sleep a thread until
 * it's ready to wake up.
 */
private[lease] object Alarm {
  /**
   * `arm` requires that a function that returns an alarm be passed to it--
   * calling apply on the function should do setup, and then also return the
   * alarm we will use for figuring out how long to sleep and when to wake up.
   */
  def arm(setup: () => Alarm) {
    val alarm = setup()
    while (!alarm.finished)
      Time.sleep(alarm.sleeptime)
  }


  /**
   * `armAndExecute` behaves similarly to `arm`, except that it interleaves the
   * sleeps with calling some side-effecting function.  This is often used for
   * logging between sleeps.  The side-effecting function will be called
   * immediately, and then for every time the alarm is found to not yet be ready
   * to finish, the alarm sleeps and also calls the function.
   */
  private[lease] def armAndExecute(setup: () => Alarm, fn: () => Unit) {
    val alarm = setup()
    fn()
    while (!alarm.finished) {
      Time.sleep(alarm.sleeptime)
      fn()
    }
  }
}

private[lease] class MinAlarm(left: Alarm, right: Alarm) extends Alarm {
  def sleeptime: Duration = left.sleeptime min right.sleeptime

  def finished: Boolean = left.finished || right.finished
}

private[lease] class DurationAlarm(dur: Duration) extends Alarm {
  private[this] val elapsed = Stopwatch.start()

  def sleeptime: Duration = dur - elapsed() max Duration.Zero
  def finished: Boolean = elapsed() >= dur
}

private[lease] class GenerationAlarm(
  ctr: ByteCounter
) extends PredicateAlarm({
  val generation = ctr.info.generation()
  () => generation != ctr.info.generation()
})

private[lease] class IntervalAlarm(val sleeptime: Duration) extends Alarm {
  def finished: Boolean = false
}

private[lease] class PredicateAlarm(pred: () => Boolean) extends Alarm {
  def sleeptime: Duration = Duration.Top
  def finished: Boolean = pred()
}

// NB: BytesAlarm will get confused without a GenerationAlarm
// when it rolls over
private[lease] class BytesAlarm(counter: ByteCounter, bytes: () => StorageUnit) extends Alarm {
  // we can refactor out the alternative minimum with an IntervalAlarm
  private[this] val P = 100  // poll period (ms)

  private[this] def target(): StorageUnit = counter.info.remaining() - bytes()

  def sleeptime: Duration = {
    val currentRate = counter.rate() // bytes per millisecond
    val targetMs = if (currentRate <= 0) P else {
      // 80% of what's predicted by rate()
      // 800 == 8 / 10 * 1000
      // 8 / 10 == 80%
      math.max((target().inBytes * 0.8 / currentRate).toLong, P / 10)
    }
    math.max(math.min(targetMs, P), 0).milliseconds
  }

  def finished: Boolean = target() <= StorageUnit.zero
}
