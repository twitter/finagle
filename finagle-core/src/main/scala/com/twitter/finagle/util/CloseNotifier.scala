package com.twitter.finagle.util

import com.twitter.util.Future

/**
 * Allows resources to register their handlers to be invoked when service is closing.
 */
trait CloseNotifier {
  def onClose(h: => Unit)
}

object CloseNotifier {

  /**
   * Creates CloseNotifier that invokes handlers in LIFO order. Methods on
   * created object are NOT thread safe.
   */
  def makeLifo(closing: Future[Unit]): CloseNotifier = new CloseNotifier {
    @volatile private[this] var closeHandlers: List[() => Unit] = Nil

    /**
     * Adds new close handler. If close event already happened,
     * handler is invoked immediately.
     */
    def onClose(h: => Unit) = {
      if (closing.isDefined)
        h
      else
        closeHandlers ::= { () => h }
    }

     // Invokes close handlers in reverse order from which they were added.
    closing ensure { closeHandlers foreach { handler =>
      handler()
    }}
  }
}