package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.service.Backoff
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

class RetryStream(underlying: Stream[Duration]) {
  @volatile private var currentStream = underlying

  def next(): Duration = synchronized {
    currentStream match {
      case nextValue #:: rest =>
        currentStream = rest
        nextValue
      case _ => 10.seconds
    }
  }

  def reset() = synchronized {
    currentStream = underlying
  }

}