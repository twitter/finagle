package com.twitter.finagle.server

import com.twitter.finagle.transport.{Transport, TransportContext}
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
trait Listener[In, Out, Ctx <: TransportContext] {
  def listen(
    addr: SocketAddress
  )(
    serveTransport: Transport[In, Out] {
      type Context <: Ctx
    } => Unit
  ): ListeningServer
}

/**
 * An empty Listener that can be used as a placeholder.
 */
object NullListener extends Listener[Any, Any, TransportContext] {
  def listen(
    addr: SocketAddress
  )(
    serveTransport: Transport[Any, Any] {
      type Context <: TransportContext
    } => Unit
  ) = NullServer
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
  case class Backlog(value: Option[Int]) {
    def mk(): (Backlog, Stack.Param[Backlog]) =
      (this, Backlog.param)
  }
  object Backlog {
    implicit val param = Stack.Param(Backlog(None))
  }

  /**
   * Configures the traffic class to be used servers.
   *
   * @param value `None` indicates no class specified. When `Some`, is an opaque
   * identifier and its meaning and interpretation are implementation specific.
   * Currently used to configure [[java.net.StandardSocketOptions.IP_TOS]].
   */
  case class TrafficClass(value: Option[Int]) {
    def mk(): (TrafficClass, Stack.Param[TrafficClass]) =
      (this, TrafficClass.param)
  }
  object TrafficClass {
    implicit val param = Stack.Param(TrafficClass(None))
  }
}
