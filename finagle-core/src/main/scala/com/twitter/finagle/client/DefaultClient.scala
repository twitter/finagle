package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.filter.{ExceptionSourceFilter, MonitorFilter}
import com.twitter.finagle.loadbalancer.{LoadBalancerFactory, DefaultBalancerFactory, WeightedLoadBalancerFactory}
import com.twitter.finagle.service.{ExpiringService, FailureAccrualFactory, FailFastFactory, TimeoutFilter}
import com.twitter.finagle.stats.{ClientStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.{DefaultMonitor, DefaultTimer, LoadedReporterFactory, ReporterFactory}
import com.twitter.util.{Duration, Monitor, Timer, Var}
import java.net.{SocketAddress, InetSocketAddress}

object DefaultClient {
  private val defaultFailureAccrual: ServiceFactoryWrapper =
    FailureAccrualFactory.wrapper(5, 5.seconds)(DefaultTimer.twitter)
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
  failureAccrual: Transformer[Req, Rep] = { factory: ServiceFactory[Req, Rep] =>
    DefaultClient.defaultFailureAccrual andThen factory
  },
  serviceTimeout: Duration = Duration.Top,
  timer: Timer = DefaultTimer.twitter,
  statsReceiver: StatsReceiver = ClientStatsReceiver,
  hostStatsReceiver: StatsReceiver = NullStatsReceiver,
  tracer: Tracer  = DefaultTracer,
  monitor: Monitor = DefaultMonitor,
  reporter: ReporterFactory = LoadedReporterFactory,
  loadBalancer: WeightedLoadBalancerFactory = DefaultBalancerFactory
) extends Client[Req, Rep] { outer =>

  private[this] def transform(stack: Stack[ServiceFactory[Req, Rep]]) = {
    val stk = stack
      .replace(FailureAccrualFactory.role, failureAccrual)
      .replace(StackClient.Role.pool, pool(statsReceiver))

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

  private[this] val underlying = new StackClient[Req, Rep](clientStack, params) {
    protected type In = Req
    protected type Out = Rep
    // The default client forces users to compose these two as part of
    // the `endpointer`. We ignore them and instead override the
    // endpointer directly.
    lazy val unimplemented = throw new Exception("unimplemented")
    val newTransporter = Function.const(unimplemented) _
    val newDispatcher = Function.const(unimplemented) _

    override protected val endpointer = new Stack.Simple[ServiceFactory[Req, Rep]] {
      val role = com.twitter.finagle.stack.Endpoint
      val description = "Send requests over the wire"
      def make(next: ServiceFactory[Req, Rep])(implicit params: Stack.Params) = {
        val param.Stats(sr) = get[param.Stats]
        val Transporter.EndpointAddr(addr) = get[Transporter.EndpointAddr]
        outer.endpointer(addr, sr)
      }
    }
  }

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
