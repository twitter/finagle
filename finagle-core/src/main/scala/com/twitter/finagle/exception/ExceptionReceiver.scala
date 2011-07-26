package com.twitter.finagle.exception

/**
 * A generic interface for receiving exceptions from Future.throw values
 * in the finagle service stack.
 */
trait ExceptionReceiver {
  def receive(e: Throwable)
}

object NullExceptionReceiver extends ExceptionReceiver {
  def receive(e: Throwable) {}
}
