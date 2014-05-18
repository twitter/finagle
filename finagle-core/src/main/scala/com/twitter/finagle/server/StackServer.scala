package com.twitter.finagle.server

import com.twitter.finagle._
import com.twitter.finagle.filter._
import com.twitter.finagle.param._
import com.twitter.finagle.service.{StatsFilter, TimeoutFilter, FailingFactory}
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stats.ServerStatsReceiver
import com.twitter.finagle.tracing.{TracingFilter, ServerDestTracingProxy}
import com.twitter.finagle.transport.Transport
import com.twitter.jvm.Jvm
import com.twitter.util.{Closable, CloseAwaitably, Future, Return, Throw, Time}
import java.net.SocketAddress
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

private[finagle] object StackServer {
  private[this] val newJvmFilter = new MkJvmFilter(Jvm())

  /**
   * Canonical Roles for each Server-related Stack modules.
   */
  object Role {
    object ServerDestTracing extends Stack.Role
    object JvmTracing extends Stack.Role
    object Preparer extends Stack.Role
  }

  /**
   * Creates a default finagle server [[com.twitter.finagle.Stack]].
   * The default stack can be configured via [[com.twitter.finagle.Stack.Param]]'s
   * in the finagle package object ([[com.twitter.finagle.param]]) and specific
   * params defined in the companion objects of the respective modules.
   *
   * @see [[com.twitter.finagle.tracing.ServerDestTracingProxy]]
   * @see [[com.twitter.finagle.filter.TimeoutFilter]]
   * @see [[com.twitter.finagle.filter.StatsFilter]]
   * @see [[com.twitter.finagle.filter.RequestSemaphoreFilter]]
   * @see [[com.twitter.finagle.filter.ExceptionSourceFilter]]
   * @see [[com.twitter.finagle.filter.MkJvmFilter]]
   * @see [[com.twitter.finagle.tracing.TracingFilter]]
   * @see [[com.twitter.finagle.filter.MonitorFilter]]
   * @see [[com.twitter.finagle.filter.HandletimeFilter]]
   */
  def newStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    val stk = new StackBuilder[ServiceFactory[Req, Rep]](
      stack.nilStack[Req, Rep])

    stk.push(Role.ServerDestTracing, ((next: ServiceFactory[Req, Rep]) => new ServerDestTracingProxy[Req, Rep](next)))
    stk.push(TimeoutFilter.module)
    stk.push(StatsFilter.module)
    stk.push(RequestSemaphoreFilter.module)
    stk.push(MaskCancelFilter.module)
    stk.push(ExceptionSourceFilter.module)
    stk.push(Role.Preparer, identity[ServiceFactory[Req, Rep]](_))
    stk.push(Role.JvmTracing, ((next: ServiceFactory[Req, Rep]) =>
      newJvmFilter[Req, Rep]() andThen next))
    stk.push(TracingFilter.module)
    stk.push(MonitorFilter.module)
    stk.push(HandletimeFilter.module)
    stk.result
  }
}

/**
 * A [[com.twitter.finagle.Stack Stack]]-based server.
 * Concrete implementations are required to define a
 * [[com.twitter.finagle.server.Listener]] and a dispatcher which
 * bridges each incoming [[com.twitter.finagle.transport.Transport]]
 * with the materialized `stack` (i.e. services produced
 * by the ServiceFactory).
 *
 * If no `stack` is provided, the default in
 * [[com.twitter.finagle.StackServer#newStack]] is used.
 */
private[finagle] abstract class StackServer[Req, Rep, In, Out](
  val stack: Stack[ServiceFactory[Req, Rep]],
  val params: Stack.Params
) extends Server[Req, Rep] { self =>
   /**
    * A convenient type alias for a server dispatcher.
    */
  type Dispatcher = (Transport[In, Out], Service[Req, Rep]) => Closable

  /**
   * Creates a new StackServer with the default stack (StackServer#newStack)
   * and [[com.twitter.finagle.stats.ServerStatsReceiver]].
   */
  def this() = this(
    StackServer.newStack[Req, Rep],
    Stack.Params.empty + Stats(ServerStatsReceiver)
  )

  /**
   * Defines a typed [[com.twitter.finagle.Listener]] for this server.
   * Concrete StackServer implementations are expected to specify this.
   */
  protected val newListener: Stack.Params => Listener[In, Out]

  /**
   * Defines a dispatcher, a function which binds a transport to a
   * [[com.twitter.finagle.Service]]. Together with a `Listener`, it
   * forms the foundation of a finagle server. Concrete implementations
   * are expected to specify this.
   *
   * @see [[com.twitter.finagle.dispatch.GenSerialServerDispatcher]]
   */
  protected val newDispatcher: Stack.Params => Dispatcher

  /**
   * Creates a new StackServer with `p` added to the `params`
   * used to configure this StackServer's `stack`.
   */
  def configured[P: Stack.Param](p: P): StackServer[Req, Rep, In, Out] =
    copy(params = params+p)

  /**
   * A copy constructor in lieu of defining StackServer as a
   * case class.
   */
  def copy(
    stack: Stack[ServiceFactory[Req, Rep]] = self.stack,
    params: Stack.Params = self.params
  ): StackServer[Req, Rep, In, Out] =
    new StackServer[Req, Rep, In, Out](stack, params) {
      protected val newListener = self.newListener
      protected val newDispatcher = self.newDispatcher
    }

  /** @inheritdoc */
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

      // Listen over `addr` and serve traffic from incoming transports to
      // `serviceFactory` via `newDispatcher`.
      val listener = newListener(serverParams)
      val dispatcher = newDispatcher(serverParams)
      val underlying = listener.listen(addr) { transport =>
        serviceFactory(newConn(transport)) respond {
          case Return(service) =>
            val d = dispatcher(transport, service)
            connections.add(d)
            transport.onClose ensure connections.remove(d)
          case Throw(_) => transport.close()
        }
      }

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

/**
 * A [[com.twitter.finagle.Stack Stack]]-based server with `Like` semantics.
 */
private[finagle]
abstract class StackServerLike[Req, Rep, In, Out, Repr <: StackServerLike[Req, Rep, In, Out, Repr]](
    server: StackServer[Req, Rep, In, Out])
  extends Server[Req, Rep]
{
  val stack = server.stack

  protected def newInstance(server: StackServer[Req, Rep, In, Out]): Repr

  def configured[P: Stack.Param](p: P): Repr =
    newInstance(server.configured(p))

  def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]) =
    server.serve(addr, factory)
}
