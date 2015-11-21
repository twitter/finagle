package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.loadbalancer.{DefaultBalancerFactory, LoadBalancerFactory}
import com.twitter.finagle.service.{
  ExpiringService, FailFastFactory, FailureAccrualFactory, TimeoutFilter}
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.finagle.stats.{ClientStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util._
import com.twitter.finagle.util.InetSocketAddressUtil.unconnected
import com.twitter.util._
import java.net.SocketAddress

object DefaultClient {
  private def defaultFailureAccrual(sr: StatsReceiver): ServiceFactoryWrapper =
    FailureAccrualFactory.wrapper(sr, FailureAccrualFactory.defaultPolicy(), "DefaultClient",  DefaultLogger, unconnected)(DefaultTimer.twitter)

  /** marker trait for uninitialized failure accrual */
  private[finagle] trait UninitializedFailureAccrual
}

/**
 * A default client implementation that does load balancing and
 * connection pooling. The only required argument is a binder,
 * responsible for binding concrete endpoints (named by
 * SocketAddresses).
 *
 * @param name A name identifying the client.
 *
 * @param endpointer A function used to create a ServiceFactory
 * to a concrete endpoint.
 *
 * @param pool The pool used to cache idle service (connection).
 *
 * @param maxIdletime The maximum time for which any `Service` is
 * permitted to be idle.
 *
 * @param maxLifetime The maximum lifetime for any `Service`
 *
 * @param requestTimeout The maximum time that any request is allowed
 * to take.
 *
 * @param failFast When enabled, installs a fail-fast module. See
 * [[com.twitter.finagle.service.FailFastFactory]]
 *
 * @param failureAccrual A failure accruing mechanism. Used to
 * gauge the health of the ServiceFactory. By default this uses
 * [[com.twitter.finagle.client.DefaultClient.defaultFailureAccrual]]
 *
 * @param serviceTimeout The maximum amount of time allowed for
 * acquiring a service. Defaults to infinity.
 */
case class DefaultClient[Req, Rep](
  name: String,
  endpointer: (SocketAddress, StatsReceiver) => ServiceFactory[Req, Rep],
  pool: StatsReceiver => Transformer[Req, Rep] = DefaultPool[Req, Rep](),
  maxIdletime: Duration = Duration.Top,
  maxLifetime: Duration = Duration.Top,
  requestTimeout: Duration = Duration.Top,
  failFast: Boolean = true,
  failureAccrual: Transformer[Req, Rep] =
    new DefaultClient.UninitializedFailureAccrual with Transformer[Req,Rep] {
        def apply(f: ServiceFactory[Req, Rep]) = f
    },
  serviceTimeout: Duration = Duration.Top,
  timer: Timer = DefaultTimer.twitter,
  statsReceiver: StatsReceiver = ClientStatsReceiver,
  hostStatsReceiver: StatsReceiver = NullStatsReceiver,
  tracer: Tracer  = DefaultTracer,
  monitor: Monitor = DefaultMonitor,
  reporter: ReporterFactory = LoadedReporterFactory,
  loadBalancer: LoadBalancerFactory = DefaultBalancerFactory,
  newTraceInitializer: Stackable[ServiceFactory[Req, Rep]] = TraceInitializerFilter.clientModule[Req, Rep]
) extends Client[Req, Rep] { outer =>

  private[this] def transform(stack: Stack[ServiceFactory[Req, Rep]]) = {
    val failureAccrualTransform: Transformer[Req,Rep] = failureAccrual match {
      case _: DefaultClient.UninitializedFailureAccrual => { factory: ServiceFactory[Req, Rep] =>
            DefaultClient.defaultFailureAccrual(statsReceiver) andThen factory
        }
      case _ => failureAccrual
    }

    val stk = stack
      .replace(FailureAccrualFactory.role, failureAccrualTransform)
      .replace(StackClient.Role.pool, pool(statsReceiver))
      .replace(TraceInitializerFilter.role, newTraceInitializer)

    if (!failFast) stk.remove(FailFastFactory.role) else stk
  }

  private[this] val clientStack = transform(StackClient.newStack[Req, Rep])
  private[this] val endpointStack = transform(StackClient.endpointStack[Req, Rep])

  private[this] val params = Stack.Params.empty +
    param.Label(name) +
    param.Timer(timer) +
    param.Monitor(monitor) +
    param.Stats(statsReceiver) +
    param.Tracer(tracer) +
    param.Reporter(reporter) +
    LoadBalancerFactory.HostStats(hostStatsReceiver) +
    LoadBalancerFactory.Param(loadBalancer) +
    TimeoutFactory.Param(serviceTimeout) +
    TimeoutFilter.Param(requestTimeout) +
    ExpiringService.Param(maxIdletime, maxLifetime)

  private[this] case class Client(stack: Stack[ServiceFactory[Req, Rep]] = clientStack,
    params: Stack.Params = params)
      extends StdStackClient[Req, Rep, Client] {

    protected def copy1(
      stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
      params: Stack.Params = this.params) = copy(stack, params)

    protected type In = Req
    protected type Out = Rep
    // The default client forces users to compose these two as part of
    // the `endpointer`. We ignore them and instead override the
    // endpointer directly.
    private[this] val unimpl = new Exception("unimplemented")
    def newTransporter() = throw unimpl
    protected def newDispatcher(transport: Transport[In, Out]) = throw unimpl

    override protected val endpointer: Stackable[ServiceFactory[Req, Rep]] =
      new Stack.Module2[Transporter.EndpointAddr, param.Stats, ServiceFactory[Req, Rep]] {
        val role = com.twitter.finagle.stack.Endpoint
        val description = "Send requests over the wire"
        def make(_addr: Transporter.EndpointAddr, _stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
          val Transporter.EndpointAddr(addr) = _addr
          val param.Stats(sr) = _stats
          outer.endpointer(addr, sr)
        }
      }
  }

  private[this] val underlying = Client()

  def newService(dest: Name, label: String) =
    underlying.newService(dest, label)

  def newClient(dest: Name, label: String) =
    underlying.newClient(dest, label)

  // These are kept around to not break the API. They probably should
  // have been private[finagle] to begin with.
  val newStack: Name => ServiceFactory[Req, Rep] = newClient(_, name)
  val newStack0: Var[Addr] => ServiceFactory[Req, Rep] = va => {
    clientStack.make(params + LoadBalancerFactory.Dest(va))
  }
  val bindStack: SocketAddress => ServiceFactory[Req, Rep] = sa => {
    endpointStack.make(params + Transporter.EndpointAddr(sa))
  }
}
