package com.twitter.finagle

import java.net.SocketAddress

/**
 * RPC clients with `Req`-typed requests and `Rep` typed replies.
 * Clients connect to [[com.twitter.finagle.Group]]s of endpoints.
 * These may be resolved dynamically through the group resolver.
 *
 * Clients are implemented by the various protocol packages in
 * finagle, for example [[com.twitter.finagle.Http]]:
 *
 * {{{
 * object Http extends Client[HttpRequest, HttpResponse] ...
 *
 * val service: Service[HttpRequest, HttpResponse] =
 *   Http.newService("google.com:80")
 * }}}
 */
trait Client[Req, Rep] {

  /**
   * Create a new Service connected to `group`.
   */
  final def newService(group: Group[SocketAddress]): Service[Req, Rep] = {
    val client = newClient(group)
    new FactoryToService[Req, Rep](client)
  }

  /**
   * Create a new servie connected to `addr`.
   */
  final def newService(addr: String): Service[Req, Rep] =
    newService(Resolver.resolve(addr)())

  /**
   * Create a new client, a `ServiceFactory` that is connected to `group`.
   */
  def newClient(group: Group[SocketAddress]): ServiceFactory[Req, Rep]

  /**
   * Create a new client, a `ServiceFactory` that is connected to the
   * group resolved by `addr`.
   */
  final def newClient(addr: String): ServiceFactory[Req, Rep] =
    newClient(Resolver.resolve(addr)())
}
