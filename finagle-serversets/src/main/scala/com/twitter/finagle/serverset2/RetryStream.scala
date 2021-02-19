package com.twitter.finagle.serverset2

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Backoff
import com.twitter.util.Duration

/**
 * Infinite stream of retry durations. Every call to `next` advances the
 * stream where a call to `reset` moves the stream back to its initial value.
 */
object RetryStream {

  val DefaultStream = Backoff.decorrelatedJittered(10.milliseconds, 10.seconds)

  def apply() =
    new RetryStream(DefaultStream)
}

class RetryStream(underlying: Backoff) {
  @volatile private var currentStream = underlying

  def next(): Duration = synchronized {
    if (currentStream.isExhausted) {
      10.seconds
    } else {
      val nextValue = currentStream.duration
      currentStream = currentStream.next
      nextValue
    }
  }

  def reset() = synchronized {
    currentStream = underlying
  }

}
