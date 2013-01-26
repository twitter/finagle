package com.twitter.finagle.server

import java.net.SocketAddress
import com.twitter.finagle.ListeningServer
import com.twitter.finagle.transport.Transport

/**
 * Listeners provide a method, `listen`, to expose a server on the
 * the given SocketAddress. `newConnection` is called for each new
 * connection. It is furnished with a typed `Transport` representing
 * this connection.
 *
 * The returned `ListeningServer` is used to inspect the server, and
 * is also used to shut it down.
 */
trait Listener[In, Out] {
  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit): ListeningServer
}
