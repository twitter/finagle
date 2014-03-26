package com.twitter.finagle.server

import com.twitter.finagle._
import com.twitter.finagle.filter._
import com.twitter.finagle.service.{StatsFilter, TimeoutFilter, FailingFactory}
import com.twitter.finagle.stats.{StatsReceiver, ServerStatsReceiver}
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
  object Roles {
    object ServerDestTracing extends Stack.Role
    object JvmTracing extends Stack.Role
    object Preparer extends Stack.Role
    object MaskCancel extends Stack.Role
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

    stk.push(Roles.ServerDestTracing, ((next: ServiceFactory[Req, Rep]) =>
      new ServerDestTracingProxy[Req, Rep](next)))
    stk.push(TimeoutFilter.module)
    stk.push(StatsFilter.module)
    stk.push(RequestSemaphoreFilter.module)
    stk.push(Roles.MaskCancel, identity[ServiceFactory[Req, Rep]](_))
    stk.push(ExceptionSourceFilter.module)
    stk.push(Roles.Preparer, identity[ServiceFactory[Req, Rep]](_))
    stk.push(Roles.JvmTracing, ((next: ServiceFactory[Req, Rep]) =>
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
 * bridges each incoming [[om.twitter.finagle.transport.Transport]]
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

  type ServerDispatcher = (Transport[In, Out], Service[Req, Rep]) => Closable

  /**
   * Creates a new StackServer with the default stack (StackServer#newStack)
   * and empty params.
   */
  def this() = this(StackServer.newStack[Req, Rep], Stack.Params.empty)

  /**
   * Defines a typed [[com.twitter.finagle.Listener]] for this server.
   * Concrete StackServer implementations are expected to specify this.
   */
  protected val listener: Listener[In, Out]

  /**
   * Defines a dispatcher, a function which binds a transport to a
   * [[com.twitter.finagle.Service]. Together with a `Listener`, it
   * forms the foundation of a finagle server. Concrete implementations
   * are expected to specify this.
   *
   * @see [[com.twitter.finagle.dispatch.GenSerialServerDispatcher]]
   */
  protected val newDispatcher: ServerDispatcher

  /**
   * Creates a new StackServer with `p` added to the `params`
   * used to configure this StackServer's `stack`.
   */
  def configured[P: Stack.Param](p: P): StackServer[Req, Rep, In, Out] =
    new StackServer[Req, Rep, In, Out](stack, params + p) {
      protected val listener = self.listener
      protected val newDispatcher = self.newDispatcher
    }

  /** @inheritdoc */
  def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]): ListeningServer =
    new ListeningServer with CloseAwaitably {
      com.twitter.finagle.Init()

      val param.Label(label) = params[param.Label]
      val param.Monitor(monitor) = params[param.Monitor]
      val param.Reporter(reporter) = params[param.Reporter]
      val statsReceiver = params[param.Stats] match {
        case param.Stats(`ServerStatsReceiver`) =>
          val scope = ServerRegistry.nameOf(addr) getOrElse label
          param.Stats(ServerStatsReceiver.scope(scope))
        case sr => sr
      }

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

      object Endpoint extends Stack.Role
      val serviceFactory = (stack ++ Stack.Leaf(Endpoint, factory)).make(
        params +
        statsReceiver +
        param.Monitor(reporter(label, None) andThen monitor)
      )

      // Listen over `addr` and serve traffic from incoming transports to
      // `serviceFactory` via `newDispatcher`.
      val underlying = listener.listen(addr) { transport =>
        serviceFactory(newConn(transport)) respond {
          case Return(service) =>
            val dispatcher = newDispatcher(transport, service)
            connections.add(dispatcher)
            transport.onClose ensure connections.remove(dispatcher)
          case Throw(_) => transport.close()
        }
      }

      protected def closeServer(deadline: Time) = closeAwaitably {
        // The order here is important: by calling underlying.close()
        // first, we guarantee that no further connections are
        // created.
        val closable = Closable.sequence(
          underlying, factory, Closable.all(connections.asScala.toSeq:_*))
        connections.clear()
        closable.close(deadline)
      }

      def boundAddress = underlying.boundAddress
    }
}

/**
 * A [[com.twitter.finagle.Stack Stack]]-based server which
 * preserves "rich" client semantics.
 */
private[finagle]
abstract class RichStackServer[Req, Rep, In, Out, This <: RichStackServer[Req, Rep, In, Out, This]](
  server: StackServer[Req, Rep, In, Out]) extends Server[Req, Rep] {
  protected def newRichServer(server: StackServer[Req, Rep, In, Out]): This
  val stack = server.stack

  def configured[P: Stack.Param](p: P): This =
    newRichServer(server.configured(p))

  def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]) =
    server.serve(addr, factory)
}