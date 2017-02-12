package com.twitter.finagle

import com.twitter.finagle.server.ServerRegistry
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.util._
import java.net.{InetSocketAddress, SocketAddress}

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

  protected[finagle] lazy val set = Var.value(Set(boundAddress))

  protected def closeServer(deadline: Time): Future[Unit]

  private[this] var isClosed = false
  private[this] var announcements = List.empty[Future[Announcement]]

  /**
   * Announce the given address and return a future to the announcement
   */
  def announce(addr: String): Future[Announcement] = synchronized {
    val public = InetSocketAddressUtil.toPublic(boundAddress).asInstanceOf[InetSocketAddress]
    if (isClosed)
      Future.exception(new Exception("Cannot announce on a closed server"))
    else {
      val ann = Announcer.announce(public, addr)
      announcements ::= ann
      ann
    }
  }

  final def close(deadline: Time): Future[Unit] = synchronized {
    isClosed = true
    val collected = Future.collect(announcements)
    collected flatMap { list =>
      Closable.all(list:_*).close(deadline) before closeServer(deadline)
    }
  }
}

/**
 * An empty ListeningServer that can be used as a placeholder. For
 * example:
 *
 * {{{
 * @volatile var server = NullServer
 * def main() { server = Http.serve(...) }
 * def exit() { server.close() }
 * }}}
 */
object NullServer extends ListeningServer with CloseAwaitably {
  def closeServer(deadline: Time) = closeAwaitably { Future.Done }
  val boundAddress = new InetSocketAddress(0)
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
 * Serve `service` at `addr`
 *
 * @define serveAndAnnounce
 *
 * Serve `service` at `addr` and announce with `name`. Announcements will be removed
 * when the service is closed. Omitting the `addr` will bind to an ephemeral port.
 */
trait Server[Req, Rep] {
  /** $addr */
  def serve(addr: SocketAddress, service: ServiceFactory[Req, Rep]): ListeningServer

  /** $addr */
  def serve(addr: SocketAddress, service: Service[Req, Rep]): ListeningServer =
    serve(addr, ServiceFactory.const(service))

  /** $addr */
  def serve(addr: String, service: ServiceFactory[Req, Rep]): ListeningServer =
    serve(ServerRegistry.register(addr), service)

  /** $addr */
  def serve(addr: String, service: Service[Req, Rep]): ListeningServer =
    serve(addr, ServiceFactory.const(service))

    /** $serveAndAnnounce */
  def serveAndAnnounce(
    name: String,
    addr: SocketAddress,
    service: ServiceFactory[Req, Rep]
  ): ListeningServer = {
    val server = serve(addr, service)
    server.announce(name)
    server
  }

  /** $serveAndAnnounce */
  def serveAndAnnounce(
    name: String,
    addr: SocketAddress,
    service: Service[Req, Rep]
  ): ListeningServer =
    serveAndAnnounce(name, addr, ServiceFactory.const(service))

  /** $serveAndAnnounce */
  def serveAndAnnounce(
    name: String,
    addr: String,
    service: ServiceFactory[Req, Rep]
  ): ListeningServer = {
    val server = serve(addr, service)
    server.announce(name)
    server
  }

  /** $serveAndAnnounce */
  def serveAndAnnounce(name: String, addr: String, service: Service[Req, Rep]): ListeningServer =
    serveAndAnnounce(name, addr, ServiceFactory.const(service))

  /** $serveAndAnnounce */
  def serveAndAnnounce(name: String, service: ServiceFactory[Req, Rep]): ListeningServer =
    serveAndAnnounce(name, ":*", service)

  /** $serveAndAnnounce */
  def serveAndAnnounce(name: String, service: Service[Req, Rep]): ListeningServer =
    serveAndAnnounce(name, ServiceFactory.const(service))
}
