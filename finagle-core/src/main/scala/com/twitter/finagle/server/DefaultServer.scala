package com.twitter.finagle.server

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.filter.{MaskCancelFilter, RequestSemaphoreFilter}
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.{StatsReceiver, ServerStatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util._
import com.twitter.finagle.{param, Stack, Stackable}
import com.twitter.finagle.{Server, Service, ServiceFactory, ListeningServer}
import com.twitter.util._
import java.net.SocketAddress

/**
 * The default Server implementation. It is given a Listener (eg.
 * [[com.twitter.finagle.netty3.Netty3Listener]]) and a function,
 * serveTransport, that binds a transport and a service. It will then
 * dispatch requests onto a standard service stack parameterized as
 * described below.
 *
 * @param listener The Listener from which to accept new typed
 * Transports.
 *
 * @param serviceTransport The function used to bind an accepted
 * Transport with a Service. Requests read from the transport are
 * dispatched onto the Service, with replies written back.
 *
 * @param requestTimeout The maximum amount of time the server is
 * allowed to handle a request. If the timeout expires, the server
 * will cancel the future and terminate the client connection.
 *
 * @param maxConcurrentRequests The maximum number of concurrent
 * requests the server is willing to handle.
 *
 * @param cancelOnHangup Enabled by default. If disabled,
 * exceptions on the transport do not propagate to the transport.
 *
 * @param prepare Prepare the given `ServiceFactory` before use.
 */
case class DefaultServer[Req, Rep, In, Out](
  name: String,
  listener: Listener[In, Out],
  serviceTransport: (Transport[In, Out], Service[Req, Rep]) => Closable,
  requestTimeout: Duration = Duration.Top,
  maxConcurrentRequests: Int = Int.MaxValue,
  cancelOnHangup: Boolean = true,
  prepare: ServiceFactory[Req, Rep] => ServiceFactory[Req, Rep] =
    (sf: ServiceFactory[Req, Rep]) => sf,
  timer: Timer = DefaultTimer.twitter,
  monitor: Monitor = DefaultMonitor,
  logger: java.util.logging.Logger = DefaultLogger,
  statsReceiver: StatsReceiver = ServerStatsReceiver,
  tracer: Tracer = DefaultTracer,
  reporter: ReporterFactory = LoadedReporterFactory,
  newTraceInitializer: Stackable[ServiceFactory[Req, Rep]] = TraceInitializerFilter.serverModule[Req, Rep]
) extends Server[Req, Rep] {

  val stack = StackServer.newStack[Req, Rep]
    .replace(StackServer.Role.preparer, prepare)
    .replace(TraceInitializerFilter.role, newTraceInitializer)

  private type _In = In
  private type _Out = Out

  private case class Server(
    stack: Stack[ServiceFactory[Req, Rep]] = stack,
    params: Stack.Params = Stack.Params.empty
  ) extends StdStackServer[Req, Rep, Server] {
    protected def copy1(
      stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
      params: Stack.Params = this.params
    ) = copy(stack, params)

    protected type In = _In
    protected type Out = _Out
    protected def newListener() = listener
    protected def newDispatcher(transport: Transport[In, Out], service: Service[Req, Rep]) =
      serviceTransport(transport, service)
  }

  val underlying: StackServer[Req, Rep] = Server()

  private[this] val sem =
    if (maxConcurrentRequests == Int.MaxValue) None
    else Some(new AsyncSemaphore(maxConcurrentRequests, 0))

  val configured = underlying
    .configured(param.Label(name))
    .configured(param.Timer(timer))
    .configured(param.Monitor(monitor))
    .configured(param.Logger(logger))
    .configured(param.Stats(statsReceiver))
    .configured(param.Tracer(tracer))
    .configured(param.Reporter(reporter))
    .configured(MaskCancelFilter.Param(!cancelOnHangup))
    .configured(TimeoutFilter.Param(requestTimeout))
    .configured(RequestSemaphoreFilter.Param(sem))

  def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]): ListeningServer =
    configured.serve(addr, factory)
}
