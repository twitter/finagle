package com.twitter.finagle.server

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{ListeningServer, NullServer, Stack}
import java.net.SocketAddress

/**
 * Listeners provide a method, `listen`, to expose a server on the
 * the given SocketAddress. `serveTransport` is called for each new
 * connection. It is furnished with a typed `Transport` representing
 * this connection.
 *
 * The returned `ListeningServer` is used to inspect the server, and
 * is also used to shut it down.
 */
trait Listener[In, Out] {
  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit): ListeningServer
}

/**
 * An empty Listener that can be used as a placeholder.
 */
object NullListener extends Listener[Any, Any] {
  def listen(addr: SocketAddress)(serveTransport: Transport[Any, Any] => Unit) = NullServer
}

/**
 * A collection of [[com.twitter.finagle.Stack.Param Stack.Params]] useful for configuring
 * a [[com.twitter.finagle.server.Listener]].
 */
object Listener {
  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure
   * the `Listener` backlog.
   *
   * @param value An option indicating the backlog size. If None,
   * the implementation default is used.
   */
  case class Backlog(value: Option[Int])
  implicit object Backlog extends Stack.Param[Backlog] {
    val default = Backlog(None)
  }
}
