package com.twitter.finagle

import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.util.{Awaitable, Closable}
import java.net.SocketAddress

/**
 * Trait ListeningServer represents a bound and listening
 * server. Closing a server instance unbinds the port and 
 * relinquishes resources that are associated with the server.
 */
trait ListeningServer 
  extends Closable 
  with Awaitable[Unit] 
  with Group[SocketAddress]
{
  /**
   * The address to which this server is bound.
   */
  def boundAddress: SocketAddress

  lazy val members = Set(boundAddress)
}

/**
 * Servers implement RPC servers with `Req`-typed requests and
 * `Rep`-typed responses. Servers dispatch requests to a
 * [[com.twitter.finagle.Service]] or
 * [[com.twitter.finagle.ServiceFactory]] provided through `serve`.
 *
 * Servers are implemented by the various protocol packages in finagle,
 * for example [[com.twitter.finagle.Http]]:
 *
 * {{{
 * object Http extends Server[HttpRequest, HttpResponse] ...
 *
 * val server = Http.serve(":*", new Service[HttpRequest, HttpResponse] {
 *   def apply(req: HttpRequest): Future[HttpResponse] = ...
 * })
 * }}}
 *
 * Will bind to an ephemeral port (":*") and dispatch request to
 * `server.boundAddress` to the provided
 * [[com.twitter.finagle.Service]] instance.
 *
 * The `serve` method has two variants: one for instances of
 * `Service`, and another for `ServiceFactory`. The `ServiceFactory`
 * variants are used for protocols in which connection state is
 * significant: a new `Service` is requested from the
 * `ServiceFactory` for each new connection, and requests on that
 * connection are dispatched to the supplied service. The service is
 * also closed when the client disconnects or the connection is
 * otherwise terminated.
 *
 * @define addr
 * 
 * Serve `service` on `addr`
 *
 * @define target
 * 
 * Serve `service` on `target`.
 */
trait Server[Req, Rep] {
  /** $addr */
  def serve(addr: SocketAddress, service: ServiceFactory[Req, Rep]): ListeningServer

  /** $addr */
  def serve(addr: SocketAddress, service: Service[Req, Rep]): ListeningServer =
    serve(addr, ServiceFactory.const(service))

  /** $target */
  def serve(target: String, service: ServiceFactory[Req, Rep]): ListeningServer =
    serve(Resolver.resolve(target), service)

  /** $target */
  def serve(target: String, service: Service[Req, Rep]): ListeningServer = {
    serve(target, ServiceFactory.const(service))
  }
}
