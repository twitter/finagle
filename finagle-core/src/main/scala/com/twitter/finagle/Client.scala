package com.twitter.finagle

import java.net.SocketAddress

/**
 * RPC clients with `Req`-typed requests and `Rep` typed replies.
 * RPC destinations are represented by names. Names are bound
 * for each request.
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
 *
 * @define newService
 *
 * Create a new service which dispatches requests to `dest`.
 *
 * @define newClient
 *
 * Create a new client connected to `dest`.
 *
 * @define label
 *
 * Argument `label` is used to assign a label to this client.
 * The label is used to display stats, etc.
 */
trait Client[Req, Rep] {

  /** $newService $label */
  def newService(dest: Name, label: String): Service[Req, Rep]

  @deprecated("Use destination names", "6.7.x")
  /** $newService */
  final def newService(dest: Group[SocketAddress]): Service[Req, Rep] =
    dest match {
      case LabelledGroup(g, label) => newService(Name.fromGroup(g), label)
      case _ => newService(Name.fromGroup(dest), "")
     }

  /** $newService */
  final def newService(dest: String): Service[Req, Rep] = {
    val (n, l) = Resolver.evalLabeled(dest)
    newService(n, l)
  }

  /** $newService */
  final def newService(dest: String, label: String): Service[Req, Rep] =
    newService(Resolver.eval(dest), label)

  /** $newClient */
  final def newClient(dest: String): ServiceFactory[Req, Rep] = {
    val (n, l) = Resolver.evalLabeled(dest)
    newClient(n, l)
  }

  /** $newClient $label */
  final def newClient(dest: String, label: String): ServiceFactory[Req, Rep] =
    newClient(Resolver.eval(dest), label)

  /** $newClient $label */
  def newClient(dest: Name, label: String): ServiceFactory[Req, Rep]

  @deprecated("Use destination names", "6.7.x")
  /** $newClient */
  final def newClient(dest: Group[SocketAddress]): ServiceFactory[Req, Rep] =
    dest match {
      case LabelledGroup(g, label) => newClient(Name.fromGroup(g), label)
      case _ => newClient(Name.fromGroup(dest), "")
    }
}
