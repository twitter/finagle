package com.twitter.finagle

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
 * See the [[https://twitter.github.io/finagle/guide/Names.html user guide]]
 * for details on destination names.
 *
 * @define newClient
 *
 * Create a new client connected to `dest`.
 * See the [[https://twitter.github.io/finagle/guide/Names.html user guide]]
 * for details on destination names.
 *
 * @define label
 *
 * Argument `label` is used to assign a label to this client.
 * The label is used to display stats, etc.
 */
trait Client[Req, Rep] {

  /** $newService $label */
  def newService(dest: Name, label: String): Service[Req, Rep]

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

}
