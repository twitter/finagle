package com.twitter.finagle.server

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{
  ClientConnection,
  ClientConnectionProxy,
  Failure,
  ListeningServer,
  Service,
  ServiceFactory
}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.{Closable, Future, Return, Throw}
import java.net.SocketAddress

/**
 * A standard template implementation for [[com.twitter.finagle.server.StackServer]]
 * that uses the transport + dispatcher pattern.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Servers.html user guide]]
 *      for further details on Finagle servers and their configuration.
 * @see [[StackServer]] for a generic representation of a stack server.
 * @see [[StackServer.newStack]] for the default modules used by Finagle
 *      servers.
 */
trait StdStackServer[Req, Rep, This <: StdStackServer[Req, Rep, This]]
    extends ListeningStackServer[Req, Rep, This] { self: This =>

  /**
   * The type we write into the transport.
   */
  protected type In

  /**
   * The type we read out of the transport.
   */
  protected type Out

  /**
   * The type of the transport's context.
   */
  protected type Context <: TransportContext

  /**
   * Defines a typed [[com.twitter.finagle.server.Listener]] for this server.
   * Concrete StackServer implementations are expected to specify this.
   */
  protected def newListener(): Listener[In, Out, Context]

  /**
   * Defines a dispatcher, a function which binds a transport to a
   * [[com.twitter.finagle.Service]]. Together with a `Listener`, it
   * forms the foundation of a finagle server. Concrete implementations
   * are expected to specify this.
   *
   * @see [[com.twitter.finagle.dispatch.GenSerialServerDispatcher]]
   *
   * @note The dispatcher must be prepared to handle the situation where it
   *       receives an already-closed transport.
   */
  protected def newDispatcher(
    transport: Transport[In, Out] {
      type Context <: self.Context
    },
    service: Service[Req, Rep]
  ): Closable

  final protected def newListeningServer(
    serviceFactory: ServiceFactory[Req, Rep],
    addr: SocketAddress
  )(
    trackSession: ClientConnection => Unit
  ): ListeningServer = {

    // Listen over `addr` and serve traffic from incoming transports to
    // `serviceFactory` via `newDispatcher`.
    val listener = newListener()

    // This assumes that the `toString` of the implementation is sufficiently descriptive.
    addServerToRegistry(listener.toString)

    listener.listen(addr) { transport =>
      val clientConnection = new ClientConnectionProxy(new TransportClientConnection(transport))

      val futureService =
        Contexts.local.let(Transport.sslSessionInfoCtx, transport.context.sslSessionInfo) {
          serviceFactory(clientConnection)
        }

      def mkSession(service: Service[Req, Rep]): Closable = {
        val dispatcher = newDispatcher(transport, service)
        // Now that we have a dispatcher, we have a higher notion of what `close(..)` does.
        // If we fail to set it in the proxy that is okay since the dispatcher is required
        // to free its resources in such an event.
        clientConnection.trySetClosable(dispatcher)
        trackSession(clientConnection)
        dispatcher
      }

      futureService.respond {
        case Return(service) =>
          mkSession(service)

        case Throw(exc) =>
          // If we fail to create a new session locally, we continue establishing
          // the session but (1) reject any incoming requests; (2) close it right
          // away. This allows protocols that support graceful shutdown to
          // also gracefully deny new sessions.
          val svc = Service.const(
            Future.exception(Failure.rejected("Terminating session and rejecting request", exc))
          )

          // We give it a generous amount of time to shut down the session to
          // improve our chances of being able to do so gracefully.
          mkSession(svc).close(10.seconds)
      }
    }
  }

}
