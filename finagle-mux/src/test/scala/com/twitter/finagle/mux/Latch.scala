package com.twitter.finagle.mux

import com.twitter.util.{Future, Promise}

/**
 * A latch is a weak, asynchronous, level-triggered condition
 * variable. It does not enforce a locking regime, so users must be
 * extra careful to flip() only under lock. It can be used as a kind of
 * asynchronous barrier.
 */
private class Latch {
  @volatile private[this] var p = new Promise[Unit]

  def get: Future[Unit] = p

  def flip(): Unit = {
    val oldp = p
    p = new Promise[Unit]
    oldp.setDone()
  }
}