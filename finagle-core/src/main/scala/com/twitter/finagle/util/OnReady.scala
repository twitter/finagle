package com.twitter.finagle.util

import com.twitter.util.Future

/**
 * A base type for something that signals its readiness asynchronously.
 */
trait OnReady {
  def onReady: Future[Unit]
}
