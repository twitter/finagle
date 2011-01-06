package com.twitter.finagle.util

import org.jboss.netty.util.Timer

import com.twitter.util.{Duration, Future, Promise, Try}

import Conversions._

class RichFuture[A](val self: Future[A]) {
  def timeout(timer: Timer, howlong: Duration, orElse: => Try[A]) = {
    val promise = new Promise[A]
    val timeout = timer(howlong) { promise.updateIfEmpty(orElse) }
    self respond { r =>
      promise.updateIfEmpty(r)
      timeout.cancel()
    }
    promise
  }
}
