package com.twitter.finagle

import com.twitter.finagle.util.InetSocketAddressUtil.unconnected
import com.twitter.util.{Closable, Future, Time}
import java.net.SocketAddress

/**
 * Information about a client, passed to a Service factory for each new
 * connection.
 */
trait ClientConnection extends Closable {

  /**
   * Host/port of the client. This is only available after `Service#connected`
   * has been signalled.
   */
  def remoteAddress: SocketAddress

  /**
   * Host/port of the local side of a client connection. This is only
   * available after `Service#connected` has been signalled.
   */
  def localAddress: SocketAddress

  /**
   * Expose a Future[Unit] that will be filled when the connection is closed
   * Useful if you want to trigger action on connection closing
   */
  def onClose: Future[Unit]
}

object ClientConnection {

  /**  A `ClientConnection` representing an unconnected client */
  val nil: ClientConnection = new ClientConnection {
    def remoteAddress: SocketAddress = unconnected
    def localAddress: SocketAddress = unconnected
    def close(deadline: Time): Future[Unit] = Future.Done
    def onClose: Future[Unit] = Future.never
  }
}
