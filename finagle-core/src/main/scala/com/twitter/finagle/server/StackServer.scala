package com.twitter.finagle.server

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.Param
import com.twitter.finagle._
import com.twitter.finagle.filter._
import com.twitter.finagle.param._
import com.twitter.finagle.service.{StatsFilter, TimeoutFilter}
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stats.ServerStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.jvm.Jvm
import com.twitter.util.{Closable, CloseAwaitably, Future, Return, Throw, Time}
import java.net.SocketAddress
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

object StackServer {
  private[this] val newJvmFilter = new MkJvmFilter(Jvm())

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
    val stk = new StackBuilder[ServiceFactory[Req, Rep]](
      stack.nilStack[Req, Rep])

    stk.push(Role.serverDestTracing, ((next: ServiceFactory[Req, Rep]) =>
      new ServerDestTracingProxy[Req, Rep](next)))
    stk.push(TimeoutFilter.serverModule)
    stk.push(DtabStatsFilter.module)
    stk.push(StatsFilter.module)
    stk.push(RequestSemaphoreFilter.module)
    stk.push(MaskCancelFilter.module)
    stk.push(ExceptionSourceFilter.module)
    stk.push(Role.jvmTracing, ((next: ServiceFactory[Req, Rep]) =>
      newJvmFilter[Req, Rep]() andThen next))
    stk.push(ServerStatsFilter.module)
    stk.push(Role.protoTracing, identity[ServiceFactory[Req, Rep]](_))
    stk.push(ServerTracingFilter.module)
    stk.push(Role.preparer, identity[ServiceFactory[Req, Rep]](_))
    // The TraceInitializerFilter must be pushed after most other modules so that
    // any Tracing produced by those modules is enclosed in the appropriate
    // span.
    stk.push(TraceInitializerFilter.serverModule)
    stk.push(MonitorFilter.module)
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
 */
trait StackServer[Req, Rep]
  extends StackBasedServer[Req, Rep]
  with Stack.Parameterized[StackServer[Req, Rep]] {

  /** The current stack used in this StackServer. */
  def stack: Stack[ServiceFactory[Req, Rep]]
  /** The current parameter map used in this StackServer */
  def params: Stack.Params
  /** A new StackServer with the provided Stack. */
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): StackServer[Req, Rep]

  def withParams(ps: Stack.Params): StackServer[Req, Rep]

  override def configured[P: Param](p: P): StackServer[Req, Rep]

  override def configured[P](psp: (P, Param[P])): StackServer[Req, Rep]
}

/**
 * A standard template implementation for
 * [[com.twitter.finagle.server.StackServer]].
 */
trait StdStackServer[Req, Rep, This <: StdStackServer[Req, Rep, This]]
  extends StackServer[Req, Rep] { self =>

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

  /**
   * Creates a new StackServer with parameter `p`.
   */
  override def configured[P: Stack.Param](p: P): This =
    withParams(params+p)

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
  ): This { type In = self.In; type Out = self.Out }

  def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]): ListeningServer =
    new ListeningServer with CloseAwaitably {
      // Ensure that we have performed global initialization.
      com.twitter.finagle.Init()

      val Monitor(monitor) = params[Monitor]
      val Reporter(reporter) = params[Reporter]
      val Stats(stats) = params[Stats]
      val Label(label) = params[Label]
      // For historical reasons, we have to respect the ServerRegistry
      // for naming addresses (i.e. label=addr). Until we deprecate
      // its usage, it takes precedence for identifying a server as
      // it is the most recently set label.
      val serverLabel = ServerRegistry.nameOf(addr) getOrElse label

      // Connection bookkeeping used to explicitly manage
      // connection resources per ListeningServer. Note, draining
      // in-flight requests is expected to be managed by `newDispatcher`,
      // so we can simply `close` all connections here.
      val connections = Collections.newSetFromMap(
        new ConcurrentHashMap[Closable, java.lang.Boolean])

      // Hydrates a new ClientConnection with connection information from the
      // given `transport`. ClientConnection instances are used to
      // thread this through a finagle server stack.
      def newConn(transport: Transport[In, Out]) = new ClientConnection {
        val remoteAddress = transport.remoteAddress
        val localAddress = transport.localAddress
        def close(deadline: Time) = transport.close(deadline)
        val onClose = transport.onClose.map(_ => ())
      }

      val statsReceiver =
        if (serverLabel.isEmpty) stats
        else stats.scope(serverLabel)

      val serverParams = params +
        Label(serverLabel) +
        Stats(statsReceiver) +
        Monitor(reporter(label, None) andThen monitor)

      val serviceFactory = (stack ++ Stack.Leaf(Endpoint, factory))
        .make(serverParams)

      val server = copy1(params=serverParams)

      // Listen over `addr` and serve traffic from incoming transports to
      // `serviceFactory` via `newDispatcher`.
      val listener = server.newListener()
      val underlying = listener.listen(addr) { transport =>
        serviceFactory(newConn(transport)) respond {
          case Return(service) =>
            val d = server.newDispatcher(transport, service)
            connections.add(d)
            transport.onClose ensure connections.remove(d)
          case Throw(exc) =>
            // If we fail to create a new session locally, we continue establishing
            // the session but (1) reject any incoming requests; (2) close it right
            // away. This allows protocols that support graceful shutdown to
            // also gracefully deny new sessions.
            val d = server.newDispatcher(
              transport, Service.const(Future.exception(Failure.rejected(exc))))
            connections.add(d)
            transport.onClose ensure connections.remove(d)
            // We give it a generous amount of time to shut down the session to
            // improve our chances of being able to do so gracefully.
            d.close(10.seconds)
        }
      }

      ServerRegistry.register(addr.toString, server.stack, server.params)

      protected def closeServer(deadline: Time) = closeAwaitably {
        // Here be dragons
        // We want to do four things here in this order:
        // 1. close the listening socket
        // 2. close the factory (not sure if ordering matters for this step)
        // 3. drain pending requests for existing connections
        // 4. close those connections when their requests complete
        // closing `underlying` eventually calls Netty3Listener.close which has an
        // interesting side-effect of synchronously closing #1
        val ulClosed = underlying.close(deadline)

        // However we don't want to wait on the above because it will only complete
        // when #4 is finished.  So we ignore it and close everything else.  Note that
        // closing the connections here will do #2 and drain them via the Dispatcher.
        val everythingElse = Seq[Closable](factory) ++ connections.asScala.toSeq

        // and once they're drained we can then wait on the listener physically closing them
        Closable.all(everythingElse:_*).close(deadline) before ulClosed
      }

      def boundAddress = underlying.boundAddress
    }
}
