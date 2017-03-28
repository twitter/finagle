package com.twitter.finagle.liveness

import com.twitter.util.{Future, Promise}

/**
 * A latch is a weak, asynchronous, level-triggered condition
 * variable. It does not enforce a locking regime, so users must be
 * extra careful to flip() only under lock. It can be used as a kind of
 * asynchronous barrier.
 */
// latch is only used for testing now, but we keep it in main because it's
// useful across testing modules.
private[finagle] class Latch {
  @volatile private[this] var p = new Promise[Unit]

  def get: Future[Unit] = p

  def flip(): Unit = {
    val oldp = p
    p = new Promise[Unit]
    oldp.setDone()
  }

  def setDone(): Unit = { p.setDone() }
}
