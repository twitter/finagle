package com.twitter.finagle.http

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.http.codec.ConnectionManager
import com.twitter.util.{Future, Time}
import java.net.SocketAddress

/**
 * A Transport with close behavior managed by ConnectionManager.
 */
class HttpTransport(
  self: Transport[Any, Any]
) extends Transport[Any, Any] {

  private[this] val manager = new ConnectionManager()

  def close(deadline: Time): Future[Unit] = self.close(deadline)

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

  def isOpen: Boolean = !manager.shouldClose && self.isOpen

  def localAddress: SocketAddress = self.localAddress

  def remoteAddress: SocketAddress = self.remoteAddress

  val onClose: Future[Throwable] = self.onClose
}
