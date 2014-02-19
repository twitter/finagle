package com.twitter.finagle.http

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.http.codec.ConnectionManager
import com.twitter.util.{Future, Time}

/**
 * A Transport with close behavior managed by ConnectionManager.
 */
class HttpTransport(
  self: Transport[Any, Any]
) extends Transport[Any, Any] {

  private[this] val manager = new ConnectionManager

  def close(deadline: Time) = self.close(deadline)

  def read(): Future[Any] =
    self.read() onSuccess { m =>
      manager.observeMessage(m)
      if (manager.shouldClose)
        self.close()
    }

  def write(m: Any): Future[Unit] = {
    manager.observeMessage(m)
    val f = self.write(m)
    if (manager.shouldClose) f before self.close()
    else f
  }

  def isOpen = !manager.shouldClose && self.isOpen

  def localAddress = self.localAddress

  def remoteAddress = self.remoteAddress

  val onClose = self.onClose
}
