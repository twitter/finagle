package com.twitter.finagle

import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo}
import com.twitter.finagle.util.InetSocketAddressUtil.unconnected
import com.twitter.util.{Closable, Future, Time}
import java.net.SocketAddress

/**
 * Information about a client, passed to a [[ServiceFactory]] for each new
 * connection.
 *
 * @note this is represented by [[ClientConnection.nil]] on the client
 *       side when it initiates a new connection.
 */
trait ClientConnection extends Closable {

  /**
   * Host/port of the client.
   */
  def remoteAddress: SocketAddress

  /**
   * Host/port of the local side of a client connection.
   */
  def localAddress: SocketAddress

  /**
   * Expose a `Future` that is satisfied when the connection is closed.
   */
  def onClose: Future[Unit]

  /**
   * SSL/TLS information associated with a client connection.
   */
  def sslSessionInfo: SslSessionInfo
}

object ClientConnection {

  /**
   * A [[ClientConnection]] representing an unconnected client.
   *
   * This is used for clients that are initiating a connection.
   *
   * @see [[ServiceFactory.apply]]
   */
  val nil: ClientConnection = new ClientConnection {
    def remoteAddress: SocketAddress = unconnected
    def localAddress: SocketAddress = unconnected
    def close(deadline: Time): Future[Unit] = Future.Done
    def onClose: Future[Unit] = Future.never
    def sslSessionInfo: SslSessionInfo = NullSslSessionInfo
  }
}
