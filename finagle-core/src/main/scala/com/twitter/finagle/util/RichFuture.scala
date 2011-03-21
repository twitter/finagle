package com.twitter.finagle.util

import com.twitter.util
import com.twitter.util.{Duration, Future, Promise, Try}

import Conversions._

private[finagle] class RichFuture[A](self: Future[A]) {
  def timeout(timer: util.Timer, howlong: Duration)(orElse: => Try[A]) = {
    val promise = new Promise[A]
    val timeout = timer.schedule(howlong.fromNow) { promise.updateIfEmpty(orElse) }
    self respond { r =>
      timeout.cancel()
      promise.updateIfEmpty(r)
    }
    promise
  }
}
