package com.twitter.finagle.http.codec

import com.twitter.finagle.http.Message
import com.twitter.util.Future

/**
 * The HTTP connection manager is a state machine that is used to determine when a connection
 * or stream is no longer useful and should be closed.
 *
 */
private[finagle] trait ConnectionManager {

  /** `Future` that is satisfied when the connection is no longer usable. */
  def onClose: Future[Unit]

  /**
   * Observe a HTTP message, along with a Future that represents the completion
   * of the messages processing. That processing may be the flushing of the message
   * to the socket or completion of the body of the respective message.
   */
  def observeMessage(message: Message, onFinish: Future[Unit]): Unit

  /** Signals whether the connection is no longer usable. */
  def shouldClose: Boolean
}
