package com.twitter.finagle.server

import com.twitter.finagle.Stack.Param
import com.twitter.finagle.filter.RequestLogger
import com.twitter.finagle.param._
import com.twitter.finagle.{ClientConnection, ListeningServer, ServiceFactory, Stack}
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stats.{
  RelativeNameMarkingStatsReceiver,
  RoleConfiguredStatsReceiver,
  Server
}
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.{CloseAwaitably, Future, Time}
import java.net.SocketAddress

/**
 * The standard template for creating a concrete representation of a [[StackServer]].
 *
 * @see [[StdStackServer]] for a further refined `StackServer` template which uses the
 *      transport + dispatcher pattern.
 */
trait ListeningStackServer[Req, Rep, This <: ListeningStackServer[Req, Rep, This]]
    extends StackServer[Req, Rep]
    with Stack.Parameterized[This]
    with Stack.Transformable[This]
    with CommonParams[This]
    with WithServerTransport[This]
    with WithServerSession[This]
    with WithServerAdmissionControl[This] { self: This =>

  /**
   * Constructs a new `ListeningServer` from the `ServiceFactory`.
   * Each new session is passed to the `trackSession` function exactly once
   * to facilitate connection resource management.
   */
  protected def newListeningServer(
    serviceFactory: ServiceFactory[Req, Rep],
    addr: SocketAddress
  )(
    trackSession: ClientConnection => Unit
  ): ListeningServer

  def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]): ListeningServer =
    new ListeningServer with CloseAwaitably {
      // Ensure that we have performed global initialization.
      com.twitter.finagle.Init()

      private[this] val monitor = params[Monitor].monitor
      private[this] val reporter = params[Reporter].reporter
      private[this] val stats = params[Stats].statsReceiver
      private[this] val label = params[Label].label
      private[this] val registry = ServerRegistry.connectionRegistry(addr)
      // For historical reasons, we have to respect the ServerRegistry
      // for naming addresses (i.e. label=addr). Until we deprecate
      // its usage, it takes precedence for identifying a server as
      // it is the most recently set label.
      private[this] val serverLabel = ServerRegistry.nameOf(addr).getOrElse(label)

      private[this] val statsReceiver =
        if (serverLabel.isEmpty) RoleConfiguredStatsReceiver(stats, Server)
        else
          RoleConfiguredStatsReceiver(
            RelativeNameMarkingStatsReceiver(stats.scope(serverLabel)),
            Server,
            Some(serverLabel))

      private[this] val serverParams = params +
        Label(serverLabel) +
        Stats(statsReceiver) +
        Monitor(reporter(label, None).andThen(monitor))

      // We re-parameterize in case `newListeningServer` needs to access the
      // finalized parameters.
      private[this] val server: This = {
        val withEndpoint = withStack(stack ++ Stack.leaf(Endpoint, factory))
        val transformed =
          params[RequestLogger.Param] match {
            case RequestLogger.Param.Enabled =>
              withEndpoint.transformed(RequestLogger.newStackTransformer(serverLabel))
            case RequestLogger.Param.Disabled =>
              withEndpoint
          }
        StackServer.DefaultTransformer.transformers.foldLeft(
          transformed.withParams(serverParams)
        )((srv, transformer) => srv.transformed(transformer))
      }

      private[this] val serviceFactory = server.stack.make(serverParams)

      // Session bookkeeping used to explicitly manage
      // session resources per ListeningServer. Note, draining
      // in-flight requests is expected to be managed by the session,
      // so we can simply `close` all sessions here.
      private[this] val sessions = new Closables

      private[this] val underlying = server.newListeningServer(serviceFactory, addr) { session =>
        registry.register(session)
        sessions.register(session)
        session.onClose.ensure {
          sessions.unregister(session)
          registry.unregister(session)
        }
      }

      ServerRegistry.register(underlying.boundAddress.toString, server.stack, server.params)

      protected def closeServer(deadline: Time): Future[Unit] = closeAwaitably {
        ServerRegistry.unregister(underlying.boundAddress.toString, server.stack, server.params)
        // Here be dragons
        // We want to do four things here in this order:
        // 1. close the listening socket
        // 2. close the factory (not sure if ordering matters for this step)
        // 3. drain pending requests for existing sessions
        // 4. close those connections when their requests complete
        //
        // Because we care about the order here, it's important that the closes
        // are done synchronously.  This means that we must be careful not to
        // schedule work for the future, as might happen if we transform or
        // respond to a future.

        // closing `underlying` eventually calls Netty3Listener.close which has an
        // interesting side-effect of synchronously closing #1
        val ulClosed = underlying.close(deadline)

        // However we don't want to wait on the above because it will only complete
        // when #4 is finished.  So we ignore it and close everything else.  Note that
        // closing the connections here will do #2 and drain them via the Dispatcher.
        val closingSessions = sessions.close(deadline)
        val closingFactory = serviceFactory.close(deadline)

        // and once they're drained we can then wait on the listener physically closing them
        Future.join(Seq(closingSessions, closingFactory)).before(ulClosed)
      }

      def boundAddress: SocketAddress = underlying.boundAddress

      override def toString: String = {
        val protocol = params[ProtocolLibrary].name
        val label = if (serverLabel.isEmpty) "<unlabeled>" else serverLabel
        s"ListeningServer($protocol, $label, $boundAddress)"
      }
    }

  /**
   * Creates a new StackServer with parameter `p`.
   */
  override def configured[P: Param](p: P): This =
    withParams(params + p)

  /**
   * Creates a new StackServer with parameter `psp._1` and Stack Param type `psp._2`.
   */
  override def configured[P](psp: (P, Stack.Param[P])): This = {
    val (p, sp) = psp
    configured(p)(sp)
  }

  /**
   * Creates a new StackServer with `params` used to configure this StackServer's `stack`.
   */
  def withParams(params: Stack.Params): This =
    copy1(params = params)

  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): This =
    copy1(stack = stack)

  override def withStack(
    fn: Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]
  ): This =
    withStack(fn(stack))

  /**
   * A copy constructor in lieu of defining StackServer as a
   * case class.
   */
  protected def copy1(
    stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
    params: Stack.Params = this.params
  ): This

  /**
   * Creates a new StackServer with additional parameters `newParams`.
   */
  override def configuredParams(newParams: Stack.Params): This = {
    withParams(params ++ newParams)
  }

  override def transformed(t: Stack.Transformer): This =
    withStack(t(stack))

  /**
   * Export info about the listener type to the global registry.
   *
   * The information about its implementation can then be queried at runtime.
   */
  final protected def addServerToRegistry(listenerName: String): Unit = {
    val listenerImplKey = Seq(
      ServerRegistry.registryName,
      params[ProtocolLibrary].name,
      params[Label].label,
      "Listener"
    )
    GlobalRegistry.get.put(listenerImplKey, listenerName)
  }
}
