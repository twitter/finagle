package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.factory.{
  BindingFactory, RefcountedFactory, StatsFactoryWrapper, TimeoutFactory}
import com.twitter.finagle.filter.{DtabStatsFilter, ExceptionSourceFilter, MonitorFilter}
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param._
import com.twitter.finagle.service._
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.{LoadedHostStatsReceiver, ClientStatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.Showable

object StackClient {
  /**
   * Canonical Roles for each Client-related Stack modules.
   */
  object Role extends Stack.Role("StackClient") {
    val pool = Stack.Role("Pool")
    val requestDraining = Stack.Role("RequestDraining")
    val prepFactory = Stack.Role("PrepFactory")
    /** PrepConn is special in that it's the first role before the `Endpoint` role */
    val prepConn = Stack.Role("PrepConn")
    val protoTracing = Stack.Role("protoTracing")
  }

  /**
   * A [[com.twitter.finagle.Stack]] representing an endpoint.
   * Note that this is terminated by a [[com.twitter.finagle.service.FailingFactory]]:
   * users are expected to terminate it with a concrete service factory.
   *
   * @see [[com.twitter.finagle.tracing.WireTracingFilter]]
   * @see [[com.twitter.finagle.service.ExpiringService]]
   * @see [[com.twitter.finagle.service.FailFastFactory]]
   * @see [[com.twitter.finagle.client.DefaultPool]]
   * @see [[com.twitter.finagle.service.TimeoutFilter]]
   * @see [[com.twitter.finagle.service.FailureAccrualFactory]]
   * @see [[com.twitter.finagle.service.StatsServiceFactory]]
   * @see [[com.twitter.finagle.service.StatsFilter]]
   * @see [[com.twitter.finagle.filter.DtabStatsFilter]]
   * @see [[com.twitter.finagle.tracing.ClientDestTracingFilter]]
   * @see [[com.twitter.finagle.filter.MonitorFilter]]
   * @see [[com.twitter.finagle.filter.ExceptionSourceFilter]]
   * @see [[com.twitter.finagle.client.LatencyCompensation]]
   */
  def endpointStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    // Ensure that we have performed global initialization.
    com.twitter.finagle.Init()

    val stk = new StackBuilder[ServiceFactory[Req, Rep]](nilStack[Req, Rep])
    stk.push(Role.prepConn, identity[ServiceFactory[Req, Rep]](_))
    stk.push(WireTracingFilter.module)
    stk.push(ExpiringService.module)
    stk.push(FailFastFactory.module)
    stk.push(DefaultPool.module)
    stk.push(TimeoutFilter.clientModule)
    stk.push(FailureAccrualFactory.module)
    stk.push(StatsServiceFactory.module)
    stk.push(StatsFilter.module)
    stk.push(DtabStatsFilter.module)
    stk.push(ClientDestTracingFilter.module)
    stk.push(MonitorFilter.module)
    stk.push(ExceptionSourceFilter.module)
    stk.push(LatencyCompensation.module)
    stk.result
  }

  /**
   * Creates a default finagle client [[com.twitter.finagle.Stack]].
   * The stack can be configured via [[com.twitter.finagle.Stack.Param]]'s
   * in the finagle package object ([[com.twitter.finagle.param]]) and specific
   * params defined in the companion objects of the respective modules.
   *
   * @see [[com.twitter.finagle.client.StackClient#endpointStack]]
   * @see [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]]
   * @see [[com.twitter.finagle.factory.StatsFactoryWrapper]]
   * @see [[com.twitter.finagle.client.StatsScoping]]
   * @see [[com.twitter.finagle.client.AddrMetadataExtraction]]
   * @see [[com.twitter.finagle.factory.BindingFactory]]
   * @see [[com.twitter.finagle.factory.RefcountedFactory]]
   * @see [[com.twitter.finagle.factory.TimeoutFactory]]
   * @see [[com.twitter.finagle.FactoryToService]]
   * @see [[com.twitter.finagle.service.Retries]]
   * @see [[com.twitter.finagle.tracing.ClientTracingFilter]]
   * @see [[com.twitter.finagle.tracing.TraceInitializerFilter]]
   */
  def newStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    /*
     * NB on orientation: we here speak of "up" / "down" or "above" /
     * "below" in terms of a request's traversal of the stack---a
     * request starts at the top and goes down, a response returns
     * back up. This is opposite to how modules are written on the
     * page; a request starts at the bottom of the `newStack` method
     * and goes up.
     */

    val stk = new StackBuilder(endpointStack[Req, Rep])

    /*
     * These modules balance requests across cluster endpoints and
     * handle automatic requeuing of failed requests.
     *
     *  * `LoadBalancerFactory` balances requests across the endpoints
     *    of a cluster given by the `LoadBalancerFactory.Dest`
     *    param. It must appear above the endpoint stack, and below
     *    `BindingFactory` in order to satisfy the
     *    `LoadBalancerFactory.Dest` param.
     *
     *  * `StatsFactoryWrapper` tracks the service acquisition latency
     *    metric. It must appear above `LoadBalancerFactory` in order
     *    to track service acquisition from the load balancer, and
     *    below `FactoryToService` so that it is called on each
     *    service acquisition.
     *
     *  * `Role.requestDraining` ensures that a service is not closed
     *    until all outstanding requests on it have completed. It must
     *    appear below `FactoryToService` so that services are not
     *    prematurely closed by `FactoryToService`. (However it is
     *    only effective for services which are called multiple times,
     *    which is never the case when `FactoryToService` is enabled.)
     *
     *  * `TimeoutFactory` times out service acquisition from
     *    `LoadBalancerFactory`. It must appear above
     *    `LoadBalancerFactory` in order to time out service
     *    acquisition from the load balancer, and below
     *    `FactoryToService` so that it is called on each service
     *    acquisition.
     *
     *  * `Role.prepFactory` is a hook used to inject codec-specific
     *    behavior; it is used in the HTTP codec to avoid closing a
     *    service while a chunked response is being read. It must
     *    appear below `FactoryToService` so that services are not
     *    prematurely closed by `FactoryToService`.
     *
     *  * `FactoryToService` acquires a new endpoint service from the
     *    load balancer on each request (and closes it after the
     *    response completes).
     *
     *  * `Retries` retries `RetryPolicy.RetryableWriteException`s
     *    automatically. It must appear above `FactoryToService` so
     *    that service acquisition failures are retried.
     */
    stk.push(LoadBalancerFactory.module)
    stk.push(StatsFactoryWrapper.module)
    stk.push(Role.requestDraining, (fac: ServiceFactory[Req, Rep]) =>
      new RefcountedFactory(fac))
    stk.push(TimeoutFactory.module)
    stk.push(Role.prepFactory, identity[ServiceFactory[Req, Rep]](_))
    stk.push(FactoryToService.module)
    stk.push(Retries.moduleRequeueable)

    /*
     * These modules deal with name resolution and request
     * distribution (when a name resolves to a `Union` of clusters).
     *
     *  * `StatsScoping` modifies the `Stats` param based on the
     *    `AddrMetadata` and `Scoper` params; it permits stats further
     *    down the stack to be scoped according to the destination
     *    cluster. It must appear below `AddrMetadataExtraction` to
     *    satisfy the `AddrMetadata` param, and above
     *    `RequeuingFilter` (and everything below it) which must have
     *    stats scoped to the destination cluster.
     *
     *  * `AddrMetadataExtraction` extracts `Addr.Metadata` from the
     *    `LoadBalancerFactory.Dest` param and puts it in the
     *    `AddrMetadata` param. (Arguably this should happen directly
     *    in `BindingFactory`.) It must appear below `BindingFactory`
     *    to satisfy the `LoadBalanceFactory.Dest param`, and above
     *    `StatsScoping` to provide the `AddrMetadata` param.
     *
     *  * `BindingFactory` resolves the destination `Name` into a
     *    `NameTree`, and distributes requests to destination clusters
     *    according to the resolved `NameTree`. Cluster endpoints are
     *    passed down in the `LoadBalancerFactory.Dest` param. It must
     *    appear above 'AddrMetadataExtraction' and
     *    `LoadBalancerFactory` to provide the
     *    `LoadBalancerFactory.Dest` param.
     *
     *  * `TimeoutFactory` times out name resolution, which happens in
     *    the service acquisition phase in `BindingFactory`; once the
     *    name is resolved, a service is acquired as soon as
     *    processing hits the `FactoryToService` further down the
     *    stack. It must appear above `BindingFactory` in order to
     *    time out name resolution, and below `FactoryToService` so
     *    that it is called on each service acquisition.
     *
     *  * `FactoryToService` acquires a new service on each request
     *    (and closes it after the response completes). This has three
     *    purposes: first, so that the per-request `Dtab.local` may be
     *    taken into account in name resolution; second, so that each
     *    request is distributed across the `NameTree`; and third, so
     *    that name resolution and request distribution are included
     *    in the request trace span. (Both name resolution and request
     *    distribution are performed in the service acquisition
     *    phase.) It must appear above `BindingFactory` and below
     *    tracing setup.
     */
    stk.push(StatsScoping.module)
    stk.push(AddrMetadataExtraction.module)
    stk.push(BindingFactory.module)
    stk.push(TimeoutFactory.module)
    stk.push(FactoryToService.module)

    /*
     * These modules set up tracing for the request span:
     *
     *  * `Role.protoTracing` is a hook for protocol-specific tracing
     *
     *  * `ClientTracingFilter` traces request send / receive
     *    events. It must appear above all other modules except
     *    `TraceInitializerFilter` so it delimits all tracing in the
     *    course of a request.
     *
     *  * `TraceInitializerFilter` allocates a new trace span per
     *    request. It must appear above all other modules so the
     *    request span encompasses all tracing in the course of a
     *    request.
     */
    stk.push(Role.protoTracing, identity[ServiceFactory[Req, Rep]](_))
    stk.push(Failure.module)
    stk.push(ClientTracingFilter.module)
    stk.push(TraceInitializerFilter.clientModule)
    stk.push(RegistryEntryLifecycle.module)
    stk.result
  }

  /**
   * The default params used for client stacks.
   */
  val defaultParams: Stack.Params =
    Stack.Params.empty +
      Stats(ClientStatsReceiver) +
      LoadBalancerFactory.HostStats(LoadedHostStatsReceiver)
}

/**
 * A [[com.twitter.finagle.Client Client]] that may have its
 * [[com.twitter.finagle.Stack Stack]] transformed.
 *
 * A `StackBasedClient` is weaker than a `StackClient` in that the
 * specific `Req`, `Rep` types of its stack are not exposed.
 */
trait StackBasedClient[Req, Rep] extends Client[Req, Rep]
  with Stack.Parameterized[StackBasedClient[Req, Rep]]
  with Stack.Transformable[StackBasedClient[Req, Rep]]

/**
 * A [[com.twitter.finagle.Client Client]] that composes a
 * [[com.twitter.finagle.Stack Stack]].
 */
trait StackClient[Req, Rep] extends StackBasedClient[Req, Rep]
  with Stack.Parameterized[StackClient[Req, Rep]]
  with Stack.Transformable[StackClient[Req, Rep]] {

  /** The current stack. */
  def stack: Stack[ServiceFactory[Req, Rep]]
  /** The current parameter map. */
  def params: Stack.Params
  /** A new StackClient with the provided stack. */
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): StackClient[Req, Rep]

  def transformed(t: Stack.Transformer): StackClient[Req, Rep] =
    withStack(t(stack))

  // these are necessary to have the right types from Java
  def withParams(ps: Stack.Params): StackClient[Req, Rep]
  def configured[P: Stack.Param](p: P): StackClient[Req, Rep]
  def configured[P](psp: (P, Stack.Param[P])): StackClient[Req, Rep]
}

/**
 * The standard template implementation for
 * [[com.twitter.finagle.client.StackClient]].
 *
 */
trait StdStackClient[Req, Rep, This <: StdStackClient[Req, Rep, This]]
    extends StackClient[Req, Rep] { self =>

  /**
   * The type we write into the transport.
   */
  protected type In

  /**
   * The type we read out of the transport.
   */
  protected type Out

  /**
   * Defines a typed [[com.twitter.finagle.client.Transporter]] for this client.
   * Concrete StackClient implementations are expected to specify this.
   */
  protected def newTransporter(): Transporter[In, Out]

  /**
   * Defines a dispatcher, a function which reconciles the stream based
   * `Transport` with a Request/Response oriented `Service`.
   * Together with a `Transporter`, it forms the foundation of a
   * finagle client. Concrete implementations are expected to specify this.
   *
   * @see [[com.twitter.finagle.dispatch.GenSerialServerDispatcher]]
   */
  protected def newDispatcher(transport: Transport[In, Out]): Service[Req, Rep]

  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): This =
    copy1(stack = stack)

  /**
   * Creates a new StackClient with `f` applied to `stack`.
   *
   * For expert users only.
   */
  def transformed(f: Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]): This =
    copy1(stack = f(stack))

  /**
   * Creates a new StackClient with parameter `p`.
   */
  override def configured[P: Stack.Param](p: P): This =
    withParams(params+p)

  /**
   * Creates a new StackClient with parameter `psp._1` and Stack Param type `psp._2`.
   */
  override def configured[P](psp: (P, Stack.Param[P])): This = {
    val (p, sp) = psp
    configured(p)(sp)
  }

  /**
   * Creates a new StackClient with `params` used to configure this StackClient's `stack`.
   */
  def withParams(params: Stack.Params): This =
    copy1(params = params)

  /**
   * Prepends `filter` to the top of the client. That is, after materializing
   * the client (newClient/newService) `filter` will be the first element which
   * requests flow through. This is a familiar chaining combinator for filters and
   * is particularly useful for `StdStackClient` implementations that don't expose
   * services but instead wrap the resulting service with a rich API.
   */
  def filtered(filter: Filter[Req, Rep, Req, Rep]): This = {
    val role = Stack.Role(filter.getClass.getSimpleName)
    val stackable = Filter.canStackFromFac.toStackable(role, filter)
    withStack(stackable +: stack)
  }

  /**
   * A copy constructor in lieu of defining StackClient as a
   * case class.
   */
  protected def copy1(
    stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
    params: Stack.Params = this.params): This { type In = self.In; type Out = self.Out }

  /**
   * A stackable module that creates new `Transports` (via transporter)
   * when applied.
   */
  protected def endpointer: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val role = Endpoint
      val description = "Send requests over the wire"
      val parameters = Seq(implicitly[Stack.Param[Transporter.EndpointAddr]])
      def make(prms: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val Transporter.EndpointAddr(addr) = prms[Transporter.EndpointAddr]
        val factory = addr match {
          case ServiceFactorySocketAddress(sf: ServiceFactory[Req, Rep]) => sf
          case _ =>
            val endpointClient = copy1(params=prms)
            val transporter = endpointClient.newTransporter()
            ServiceFactory(() => transporter(addr).map(endpointClient.newDispatcher))
        }
        Stack.Leaf(this, factory)
      }
    }

  def newClient(dest: Name, label0: String): ServiceFactory[Req, Rep] = {
    val Stats(stats) = params[Stats]
    val Label(label1) = params[Label]

    // For historical reasons, we have two sources for identifying
    // a client. The most recently set `label0` takes precedence.
    val clientLabel = (label0, label1) match {
      case ("", "") => Showable.show(dest)
      case ("", l1) => l1
      case (l0, l1) => l0
    }

    val clientStack = stack ++ (endpointer +: nilStack)
    val clientParams = (params +
      Label(clientLabel) +
      Stats(stats.scope(clientLabel)) +
      BindingFactory.Dest(dest))

    clientStack.make(clientParams)
  }

  override def newService(dest: Name, label: String): Service[Req, Rep] = {
    val client = copy1(
      params = params + FactoryToService.Enabled(true)
    ).newClient(dest, label)
    new FactoryToService[Req, Rep](client)
  }
}
