package com.twitter.finagle

import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.util.{Closable, Future, Time}
import java.net.SocketAddress

/**
 * Proxy implementation of [[ClientConnection]] which allows the
 * target of `close` to be redirected.
 *
 * This is useful for situations where we get increasingly sophisticated
 * `close` behavior, such as when we start with a `Transport` and in the
 * future, add a `Dispatcher` to it which may have a more interesting
 * notion of `close`.
 */
private class ClientConnectionProxy(underlying: ClientConnection) extends ClientConnection {

  // The thread safety of these variables is provided by synchronization on `this`
  private[this] var closable: Closable = underlying
  private[this] var closeCalled = false

  /**
   * Attempts to replace the target of `close()` calls
   *
   * @return true if `close` has not already been called, in which case the
   *         methods argument becomes the new target of `close()` calls,
   *         false otherwise.
   */
  def trySetClosable(c: Closable): Boolean = synchronized {
    if (closeCalled) false
    else {
      closable = c
      true
    }
  }

  /**
   * Close's the currently targeted `Closable`
   *
   * After the first invocation, the close target can no longer be changed
   * but multiple invocations of `close` will be passed to the target.
   */
  def close(deadline: Time): Future[Unit] = {
    val toClose = synchronized {
      closeCalled = true
      closable
    }

    toClose.close(deadline)
  }

  def remoteAddress: SocketAddress = underlying.remoteAddress
  def localAddress: SocketAddress = underlying.localAddress
  def onClose: Future[Unit] = underlying.onClose
  def sslSessionInfo: SslSessionInfo = underlying.sslSessionInfo
}
