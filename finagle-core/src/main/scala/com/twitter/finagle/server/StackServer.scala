package com.twitter.finagle.server

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.filter._
import com.twitter.finagle.param._
import com.twitter.finagle.service.{ExpiringService, StatsFilter, TimeoutFilter}
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.Stack.{Role, Param}
import com.twitter.finagle.stats.ServerStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.jvm.Jvm
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.{Closable, CloseAwaitably, Future, Return, Throw, Time}
import java.net.SocketAddress

object StackServer {

  private[this] lazy val newJvmFilter = new MkJvmFilter(Jvm())

  private[this] class JvmTracing[Req, Rep] extends Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
    override def role: Role = Role.jvmTracing
    override def description: String = "Server-side JVM tracing"
    override def make(_tracer: param.Tracer, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
      val param.Tracer(tracer) = _tracer
      if (tracer.isNull) next
      else newJvmFilter[Req, Rep].andThen(next)
    }
  }

  /**
   * Canonical Roles for each Server-related Stack modules.
   */
  object Role extends Stack.Role("StackServer") {
    val serverDestTracing = Stack.Role("ServerDestTracing")
    val jvmTracing = Stack.Role("JvmTracing")
    val preparer = Stack.Role("preparer")
    val protoTracing = Stack.Role("protoTracing")
   }

  /**
   * Creates a default finagle server [[com.twitter.finagle.Stack]].
   * The default stack can be configured via [[com.twitter.finagle.Stack.Param]]'s
   * in the finagle package object ([[com.twitter.finagle.param]]) and specific
   * params defined in the companion objects of the respective modules.
   *
   * @see [[com.twitter.finagle.tracing.ServerDestTracingProxy]]
   * @see [[com.twitter.finagle.service.TimeoutFilter]]
   * @see [[com.twitter.finagle.filter.DtabStatsFilter]]
   * @see [[com.twitter.finagle.service.StatsFilter]]
   * @see [[com.twitter.finagle.filter.RequestSemaphoreFilter]]
   * @see [[com.twitter.finagle.filter.ExceptionSourceFilter]]
   * @see [[com.twitter.finagle.filter.MkJvmFilter]]
   * @see [[com.twitter.finagle.tracing.ServerTracingFilter]]
   * @see [[com.twitter.finagle.tracing.TraceInitializerFilter]]
   * @see [[com.twitter.finagle.filter.MonitorFilter]]
   * @see [[com.twitter.finagle.filter.ServerStatsFilter]]
   */
  def newStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    val stk = new StackBuilder[ServiceFactory[Req, Rep]](stack.nilStack[Req, Rep])

    // We want to start expiring services as close to their instantiation
    // as possible. By installing `ExpiringService` here, we are guaranteed
    // to wrap the server's dispatcher.
    stk.push(ExpiringService.server)
    stk.push(Role.serverDestTracing, ((next: ServiceFactory[Req, Rep]) =>
      new ServerDestTracingProxy[Req, Rep](next)))
    stk.push(TimeoutFilter.serverModule)
    stk.push(DtabStatsFilter.module)
    // Admission Control filters are inserted after `StatsFilter` so that rejected
    // requests are counted. We may need to adjust how latency are recorded
    // to exclude Nack response from latency stats, CSL-2306.
    stk.push(ServerAdmissionControl.module)
    stk.push(StatsFilter.module)
    stk.push(RequestSemaphoreFilter.module)
    stk.push(MaskCancelFilter.module)
    stk.push(ExceptionSourceFilter.module)
    stk.push(new JvmTracing)
    stk.push(ServerStatsFilter.module)
    stk.push(Role.protoTracing, identity[ServiceFactory[Req, Rep]](_))
    stk.push(ServerTracingFilter.module)
    stk.push(Role.preparer, identity[ServiceFactory[Req, Rep]](_))
    // The TraceInitializerFilter must be pushed after most other modules so that
    // any Tracing produced by those modules is enclosed in the appropriate
    // span.
    stk.push(TraceInitializerFilter.serverModule)
    stk.push(MonitorFilter.serverModule)
    stk.result
  }

  /**
   * The default params used for StackServers.
   */
  val defaultParams: Stack.Params =
    Stack.Params.empty + Stats(ServerStatsReceiver)
}

/**
 * A [[com.twitter.finagle.Server Server]] that is
 * parameterized.
 */
trait StackBasedServer[Req, Rep]
  extends Server[Req, Rep]
  with Stack.Parameterized[StackBasedServer[Req, Rep]]

/**
 * A [[com.twitter.finagle.Server]] that composes a
 * [[com.twitter.finagle.Stack]].
 *
 * @see [[ListeningServer]] for a template implementation that tracks session resources.
 */
trait StackServer[Req, Rep]
  extends StackBasedServer[Req, Rep]
  with Stack.Parameterized[StackServer[Req, Rep]] {

  /** The current stack used in this StackServer. */
  def stack: Stack[ServiceFactory[Req, Rep]]

  /** The current parameter map used in this StackServer. */
  def params: Stack.Params

  /** A new StackServer with the provided Stack. */
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): StackServer[Req, Rep]

  def withParams(ps: Stack.Params): StackServer[Req, Rep]

  override def configured[P: Param](p: P): StackServer[Req, Rep]

  override def configured[P](psp: (P, Param[P])): StackServer[Req, Rep]

  override def configuredParams(params: Stack.Params): StackServer[Req, Rep]
}

/**
 * The standard template for creating a concrete representation of a [[StackServer]].
 *
 * @see [[StdStackServer]] for a further refined `StackServer` template which uses the
 *      transport + dispatcher pattern.
 */
trait ListeningStackServer[Req, Rep, This <: ListeningStackServer[Req, Rep, This]]
  extends StackServer[Req, Rep]
  with Stack.Parameterized[This]
  with CommonParams[This]
  with WithServerTransport[This]
  with WithServerSession[This]
  with WithServerAdmissionControl[This] { self: This =>

  /**
   * Constructs a new `ListeningServer` from the `ServiceFactory`.
   * Each new session is passed to the `trackSession` function exactly once
   * to facilitate connection resource management.
   */
  protected def newListeningServer(serviceFactory: ServiceFactory[Req, Rep], addr: SocketAddress)(trackSession: ClientConnection => Unit): ListeningServer

  final def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]): ListeningServer =
    new ListeningServer with CloseAwaitably {
      // Ensure that we have performed global initialization.
      com.twitter.finagle.Init()

      val Monitor(monitor) = params[Monitor]
      val Reporter(reporter) = params[Reporter]
      val Stats(stats) = params[Stats]
      val Label(label) = params[Label]
      val registry = ServerRegistry.connectionRegistry(addr)
      // For historical reasons, we have to respect the ServerRegistry
      // for naming addresses (i.e. label=addr). Until we deprecate
      // its usage, it takes precedence for identifying a server as
      // it is the most recently set label.
      val serverLabel = ServerRegistry.nameOf(addr).getOrElse(label)

      val statsReceiver =
        if (serverLabel.isEmpty) stats
        else stats.scope(serverLabel)

      val serverParams = params +
        Label(serverLabel) +
        Stats(statsReceiver) +
        Monitor(reporter(label, None) andThen monitor)

      val serviceFactory = (stack ++ Stack.Leaf(Endpoint, factory))
        .make(serverParams)

      // We re-parameterize in case `newListeningServer` needs to access the
      // finalized parameters.
      val server = withParams(serverParams)

      // Session bookkeeping used to explicitly manage
      // session resources per ListeningServer. Note, draining
      // in-flight requests is expected to be managed by the session,
      // so we can simply `close` all sessions here.
      val sessions = new Closables

      val underlying = server.newListeningServer(serviceFactory, addr) { session =>
        registry.register(session.remoteAddress)
        sessions.register(session)
        session.onClose.ensure {
          sessions.unregister(session)
          registry.unregister(session.remoteAddress)
        }
      }

      ServerRegistry.register(underlying.boundAddress.toString, server.stack, server.params)

      protected def closeServer(deadline: Time) = closeAwaitably {
        // Here be dragons
        // We want to do four things here in this order:
        // 1. close the listening socket
        // 2. close the factory (not sure if ordering matters for this step)
        // 3. drain pending requests for existing sessions
        // 4. close those connections when their requests complete
        // closing `underlying` eventually calls Netty3Listener.close which has an
        // interesting side-effect of synchronously closing #1
        val ulClosed = underlying.close(deadline)

        // However we don't want to wait on the above because it will only complete
        // when #4 is finished.  So we ignore it and close everything else.  Note that
        // closing the connections here will do #2 and drain them via the Dispatcher.
        val closingSessions = sessions.close(deadline)
        val closingFactory = factory.close(deadline)

        // and once they're drained we can then wait on the listener physically closing them
        Future.join(Seq(closingSessions, closingFactory)).before(ulClosed)
      }

      def boundAddress = underlying.boundAddress
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
}

/**
 * A standard template implementation for [[com.twitter.finagle.server.StackServer]]
 * that uses the transport + dispatcher pattern.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Servers.html user guide]]
 *      for further details on Finagle servers and their configuration.
 *
 * @see [[StackServer]] for a generic representation of a stack server.
 *
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
   * Defines a typed [[com.twitter.finagle.server.Listener]] for this server.
   * Concrete StackServer implementations are expected to specify this.
   */
  protected def newListener(): Listener[In, Out]

  /**
   * Defines a dispatcher, a function which binds a transport to a
   * [[com.twitter.finagle.Service]]. Together with a `Listener`, it
   * forms the foundation of a finagle server. Concrete implementations
   * are expected to specify this.
   *
   * @see [[com.twitter.finagle.dispatch.GenSerialServerDispatcher]]
   */
  protected def newDispatcher(transport: Transport[In, Out], service: Service[Req, Rep]): Closable

  final protected def newListeningServer(
    serviceFactory: ServiceFactory[Req, Rep],
    addr: SocketAddress
  )(trackSession: ClientConnection => Unit): ListeningServer = {

    // Listen over `addr` and serve traffic from incoming transports to
    // `serviceFactory` via `newDispatcher`.
    val listener = newListener()

    // Export info about the listener type so that we can query info
    // about its implementation at runtime. This assumes that the `toString`
    // of the implementation is sufficiently descriptive.
    val listenerImplKey = Seq(
      ServerRegistry.registryName,
      params[ProtocolLibrary].name,
      params[Label].label,
      "Listener")
    GlobalRegistry.get.put(listenerImplKey, listener.toString)

    listener.listen(addr) { transport =>
      val clientConnection = new ClientConnectionImpl(transport)
      val futureService = transport.peerCertificate match {
        case None => serviceFactory(clientConnection)
        case Some(cert) => Contexts.local.let(Transport.peerCertCtx, cert) {
          serviceFactory(clientConnection)
        }
      }
      futureService.respond {
        case Return(service) =>
          val d = newDispatcher(transport, service)
          // Now that we have a dispatcher, we have a higher notion of what `close(..)` does, so use it
          clientConnection.setClosable(d)
          trackSession(clientConnection)

        case Throw(exc) =>
          // If we fail to create a new session locally, we continue establishing
          // the session but (1) reject any incoming requests; (2) close it right
          // away. This allows protocols that support graceful shutdown to
          // also gracefully deny new sessions.
          val d = newDispatcher(
            transport,
            Service.const(Future.exception(
              Failure.rejected("Terminating session and ignoring request", exc)))
          )

          // Now that we have a dispatcher, we have a higher notion of what `close(..)` does, so use it
          clientConnection.setClosable(d)
          trackSession(clientConnection)
          // We give it a generous amount of time to shut down the session to
          // improve our chances of being able to do so gracefully.
          d.close(10.seconds)
      }
    }
  }

  private class ClientConnectionImpl(t: Transport[In, Out]) extends ClientConnection {
    @volatile
    private var closable: Closable = t

    def setClosable(closable: Closable): Unit = {
      this.closable = closable
    }

    override def remoteAddress: SocketAddress = t.remoteAddress
    override def localAddress: SocketAddress = t.localAddress
    // In the Transport + Dispatcher model, the Transport is a source of truth for
    // the `onClose` future: closing the dispatcher will result in closing the
    // Transport and closing the Transport will trigger shutdown of the dispatcher.
    // Therefore, even when we swap the closable that is the target of `this.close(..)`,
    // they both will complete the transports `onClose` future.
    override val onClose: Future[Unit] = t.onClose.unit
    override def close(deadline: Time): Future[Unit] = closable.close(deadline)
  }
}
