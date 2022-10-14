package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.context
import com.twitter.finagle.factory.RefcountedFactory
import com.twitter.finagle.factory.StatsFactoryWrapper
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.filter._
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.param._
import com.twitter.finagle.service._
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.ClientStatsReceiver
import com.twitter.finagle.stats.LoadedHostStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.util.registry.GlobalRegistry

object StackClient {

  object RequestDraining {
    def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
      new Stack.Module0[ServiceFactory[Req, Rep]] {
        val role: Stack.Role = Role.requestDraining
        val description: String =
          "Ensures that services wait until all outstanding requests complete before closure"
        def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
          new RefcountedFactory(next)
      }
  }

  object PrepFactory {
    def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
      new Stack.Module0[ServiceFactory[Req, Rep]] {
        val role: Stack.Role = Role.prepFactory
        val description: String =
          """A pre-allocated module at the top of the stack (after name resolution).
            | Injects codec-specific behavior during service acquisition""".stripMargin
        def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = next
      }
  }

  object PrepConn {
    def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
      new Stack.Module0[ServiceFactory[Req, Rep]] {
        val role: Stack.Role = Role.prepConn
        val description: String =
          "Pre-allocated module at the bottom of the stack that handles newly connected sessions"
        def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = next
      }
  }

  object ProtoTracing {
    def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
      new Stack.Module0[ServiceFactory[Req, Rep]] {
        val role: Stack.Role = Role.protoTracing
        val description: String =
          "Pre-allocated stack module for protocols to inject tracing"
        def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = next
      }
  }

  /**
   * Canonical Roles for some Client-related Stack module. Other roles are defined
   * within the companion objects of the respective modules.
   */
  object Role extends Stack.Role("StackClient") {

    /**
     * Defines the role of the connection pool in the client stack.
     */
    val pool = Stack.Role("Pool")

    /**
     * Defines the role of the [[RefcountedFactory]] in the client stack.
     */
    val requestDraining = Stack.Role("RequestDraining")

    /**
     * Defines a preallocated position at the "top" of the stack (after name resolution)
     * which allows the injection of codec-specific behavior during service acquisition.
     * For example, it is  used in the HTTP codec to avoid closing a service while a
     * chunked response is being read.
     */
    val prepFactory = Stack.Role("PrepFactory")

    /**
     * Defines the role of the module responsible for the service acquisition
     * timeout for name resolution in the client stack.
     */
    val nameResolutionTimeout = Stack.Role("NameResolutionTimeout")

    /**
     * Defines the role of the module responsible for the service acquisition
     * timeout after name resolution is complete. This encompasses the timeout
     * for establishing a new session (e.g. handshaking), outside of name resolution.
     */
    val postNameResolutionTimeout = Stack.Role("PostNameResolutionTimeout")

    /**
     * Defines a pre-allocated position at the "bottom" of the stack which is
     * special in that it's the first role before the client sends the request to
     * the underlying transport implementation.
     */
    val prepConn = Stack.Role("PrepConn")

    /**
     * Defines a pre-allocated position in the stack for protocols to inject tracing.
     */
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
   * @see [[com.twitter.finagle.service.PendingRequestFilter]]
   * @see [[com.twitter.finagle.client.DefaultPool]]
   * @see [[com.twitter.finagle.service.ClosableService]]
   * @see [[com.twitter.finagle.service.TimeoutFilter]]
   * @see [[com.twitter.finagle.liveness.FailureAccrualFactory]]
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

    /**
     * N.B. see the note in `newStack` regarding up / down orientation in the stack.
     */
    val stk = new StackBuilder[ServiceFactory[Req, Rep]](nilStack[Req, Rep])

    /**
     * `OffloadFilter` shifts future continuations (callbacks and
     * transformations) off of IO threads into a configured `FuturePool`.
     * This module is intentionally placed at the top of the stack
     * such that execution context shifts as client's response leaves
     * the stack and enters the application code.
     */
    stk.push(OffloadFilter.client)

    /**
     * `prepConn` is the bottom of the stack by definition. This position represents
     * the first module to handle newly connected sessions.
     *
     * finagle-thrift uses this role to install session upgrading logic from
     * vanilla Thrift to Twitter Thrift.
     */
    stk.push(PrepConn.module)

    /**
     * `WriteTracingFilter` annotates traced requests. Annotations are timestamped
     * so this should be low in the stack to accurately delineate between wire time
     * and handling time.
     */
    stk.push(WireTracingFilter.clientModule)

    /**
     * `ExpiringService` enforces an idle timeout and total ttl for connections.
     * This module must be beneath the DefaultPool in order to apply per connection.
     *
     * N.B. the difference between this connection ttl and the `DefaultPool` ttl
     * (via CachingPool) is that this applies to *all* connections and `DefaultPool`
     * only expires connections above the low watermark.
     */
    stk.push(ExpiringService.client)

    /**
     * `FailFastFactory` accumulates failures per connection, marking the endpoint
     * as unavailable so that modules higher in the stack can dispatch requests
     * around the failing endpoint.
     */
    stk.push(FailFastFactory.module)

    /**
     * `PendingRequestFilter` enforces a limit on the number of pending requests
     * for a single connection. It must be beneath the `DefaultPool` module so that
     * its limits are applied per connection rather than per endpoint.
     */
    stk.push(PendingRequestFilter.module)

    /**
     * `DefaultPool` configures connection pooling. Like the `LoadBalancerFactory`
     * module it is a potentially aggregate [[ServiceFactory]] composed of multiple
     * [[Service Services]] which represent a distinct session to the same endpoint.
     *
     * When the service lifecycle is managed independently of the stack. `ClosableService`
     * ensures a closed service cannot be reused. Typically a closed service releases
     * its connection into the configured pool.
     */
    stk.push(DefaultPool.module)
    stk.push(ClosableService.client)

    /**
     * `TimeoutFilter` enforces static request timeouts and broadcast request deadlines,
     * sending a best-effort interrupt for expired requests.
     * It must be beneath the `StatsFilter` so that timeouts are properly recorded.
     */
    stk.push(TimeoutFilter.clientModule)

    /**
     * `FailureAccrualFactory` accrues request failures per endpoint updating its
     * status so that modules higher in the stack may route around an unhealthy
     * endpoint.
     *
     * It must be above `DefaultPool` to accumulate failures across all sessions
     * to an endpoint.
     * It must be above `TimeoutFilter` so that it can observe request timeouts.
     * It must be above `PendingRequestFilter` so that it can observe client
     * admission rejections.
     */
    stk.push(FailureAccrualFactory.module)

    /**
     * `ExceptionRemoteInfoFactory` fills in remote info (upstream addr/client id,
     * downstream addr/client id, and trace id) in exceptions. This needs to be near the top
     * of the stack so that failures anywhere lower in the stack have remote
     * info added to them, but below the stats, tracing, and monitor filters so these filters
     * see exceptions with remote info added.
     */
    stk.push(ExceptionRemoteInfoFactory.module)

    /**
     * `StatsServiceFactory` exports a gauge which reports the status of the stack
     * beneath it. It must be above `FailureAccrualFactory` in order to record
     * failure accrual's aggregate view of health over multiple requests.
     */
    stk.push(StatsServiceFactory.module)

    /**
     * `StatsFilter` installs a (wait for it...) stats filter on active sessions.
     * It must be above the `TimeoutFilter` so that it can record timeouts as failures.
     * It has no other position constraint.
     */
    stk.push(StatsFilter.module)

    /**
     * `DtabStatsFilter` exports dtab stats. It has no relative position constraints
     * within the endpoint stack.
     */
    stk.push(DtabStatsFilter.module)

    /**
     * `ClientDestTracingFilter` annotates the trace with the destination endpoint's
     * socket address. It has no position constraints within the endpoint stack.
     */
    stk.push(ClientDestTracingFilter.module)

    /**
     * `MonitorFilter` installs a configurable exception handler ([[Monitor]]) for
     * client sessions. There is no specific position constraint but higher in the
     * stack is preferable so it can wrap more application logic.
     */
    stk.push(MonitorFilter.clientModule)

    /**
     * `LatencyCompensation` configures latency compensation based on destination.
     *
     * It must appear above consumers of the the c.t.f.client.Compensation param, so
     * above `TimeoutFilter`.
     *
     * It is only evaluated at stack creation time.
     */
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
   * @see [[com.twitter.finagle.naming.BindingFactory]]
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
     *
     * Also note that the term "stack" does not refer to a stack in the
     * computer science sense but instead in the sense of a chain of objects,
     * i.e., stack modules. Because modules are composed sequentially, it also
     * makes sense to speak of modules coming "before" or "after" others.
     *
     * Lastly, note that "module A comes before module B" has the same meaning
     * as "module A is pushed after module B".
     */

    val stk = new StackBuilder(endpointStack[Req, Rep])

    /*
     * These modules balance requests across cluster endpoints and
     * handle automatic requeuing of failed requests.
     *  * `ExportSslUsage` exports the TLS parameter to the R* Registry
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
     *  * `RequestDraining` ensures that a service is not closed
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
     *  * `PrepFactory` is a hook used to inject codec-specific
     *    behavior that needs to run for each session before it's been acquired.
     *
     *  * `FactoryToService` acquires a new endpoint service from the
     *    load balancer on each request (and closes it after the
     *    response completes).
     *
     *  * `Retries` retries `RetryPolicy.RetryableWriteException`s
     *    automatically. It must appear above `FactoryToService` so
     *    that service acquisition failures are retried.
     *
     *  * `ClearContextValueFilter` clears the configured Context key,
     *    `Retries`, in the request's Context. This module must
     *    come before `Retries` so that it doesn't clear the `Retries`
     *    set by this client. `Retries` is only meant to be propagated
     *    one hop from the client to the server. The client overwrites `Retries`
     *    in the `RequeueFilter` with its own value; however, if the client
     *    has removed `Retries` in its configuration, we want `Retries`
     *    to be cleared so the server doesn't see a value set by another client.
     *
     *  * `ExceptionSourceFilter` is the exception handler of last resort. It recovers
     *     application errors into failed [[Future Futures]] and attributes the failures to
     *     clients by client label. This needs to be near the top of the stack so that
     *     request failures anywhere lower in the stack have endpoints attributed to them.
     */
    stk.push(ExportSslUsage.module)
    stk.push(LoadBalancerFactory.module)
    stk.push(StatsFactoryWrapper.module)
    stk.push(RequestDraining.module)
    stk.push(TimeoutFactory.module(Role.postNameResolutionTimeout))
    stk.push(PrepFactory.module)
    stk.push(FactoryToService.module)
    stk.push(Retries.moduleRequeueable)
    stk.push(ClearContextValueFilter.module(context.Retries))
    stk.push(ExceptionSourceFilter.module)

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
     *  * `EndpointRecorder` passes endpoint information to the
     *    `EndpointRegistry`. It must appear below `BindingFactory` so
     *    `BindingFactory` can set the `Name.Bound` `BindingFactory.Dest`
     *    param.
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
    stk.push(EndpointRecorder.module)
    stk.push(BindingFactory.module)
    stk.push(TimeoutFactory.module(Role.nameResolutionTimeout))
    stk.push(FactoryToService.module)

    /*
     * These modules set up tracing for the request span and miscellaneous
     * actions before a request leaves the client stack:
     *
     *  * `ProtoTracing` is a hook for protocol-specific tracing
     *
     *  * `Failure` processes request failures for external representation
     *
     *  * `ClientTracingFilter` traces request send / receive
     *    events. It must appear above all other modules except
     *    `TraceInitializerFilter` so it delimits all tracing in the
     *    course of a request.
     *
     *  * `ForwardAnnotation` allows us to inject annotations into child
     *    span.
     *
     *  * `RegistryEntryLifecycle` is a hook for registering this client
     *    into the ClientRegistry and removing on close.
     *
     *  * `OffloadFilter` shifts future continuations (callbacks and
     *    transformations) off of IO threads into a configured `FuturePool`.
     *    This module is intentionally placed at the top of the stack
     *    such that execution context shifts as client's response leaves
     *    the stack and enters the application code.
     *
     *  * `ClientExceptionTracingFilter` reports binary annotations for exceptions.
     *    It has no position constraints within the stack.
     *
     *  * `TraceInitializerFilter` allocates a new trace span per
     *    request. It must appear above all other modules so the
     *    request span encompasses all tracing in the course of a
     *    request.
     */
    stk.push(ProtoTracing.module)
    stk.push(Failure.module)
    stk.push(ClientTracingFilter.module)
    stk.push(ForwardAnnotation.module)
    stk.push(RegistryEntryLifecycle.module)
    stk.push(ClientExceptionTracingFilter.module())
    stk.push(TraceInitializerFilter.clientModule())
    stk.result
  }

  /**
   * The default params used for client stacks.
   */
  def defaultParams: Stack.Params =
    Stack.Params.empty +
      Stats(ClientStatsReceiver) +
      LoadBalancerFactory.HostStats(LoadedHostStatsReceiver) +
      MetricBuilders(Some(new CoreMetricsRegistry())) +
      // RetryBudget needs to be shared across one physical client.
      Retries.Budget.default

  /**
   * A set of StackTransformers for transforming client stacks.
   */
  private[twitter] object DefaultTransformer
      extends StackTransformerCollection[ClientStackTransformer]

  /**
   * A set of ClientParamsInjectors for transforming client params.
   */
  private[finagle] object DefaultInjectors extends ParamsInjectorCollection[ClientParamsInjector]
}

/**
 * A [[com.twitter.finagle.Client Client]] that composes a
 * [[com.twitter.finagle.Stack Stack]].
 */
trait StackClient[Req, Rep]
    extends StackBasedClient[Req, Rep]
    with Stack.Parameterized[StackClient[Req, Rep]]
    with Stack.Transformable[StackClient[Req, Rep]] {

  /**
   * Export info about the transporter type so that we can query info
   * about its implementation at runtime.
   */
  final protected def registerTransporter(transporterName: String): Unit = {
    val transporterImplKey = Seq(
      ClientRegistry.registryName,
      params[ProtocolLibrary].name,
      params[Label].label,
      "Transporter"
    )
    GlobalRegistry.get.put(transporterImplKey, transporterName)
  }

  /** The current stack. */
  def stack: Stack[ServiceFactory[Req, Rep]]

  /** The current parameter map. */
  def params: Stack.Params

  /**
   * A new [[StackClient]] with the provided `stack`.
   *
   * @see `withStack` that takes a `Function1` for a more ergonomic
   *     API when used with method chaining.
   */
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): StackClient[Req, Rep]

  /**
   * A new [[StackClient]] using the function to create a new [[Stack]].
   *
   * The input to `fn` is the [[stack client's current stack]].
   * This API allows for easier usage when writing code that
   * uses method chaining.
   *
   * This method is similar to [[transformed]] while providing easier API
   * ergonomics for one-off `Stack` changes.
   *
   * @example
   * From Scala:
   * {{{
   * import com.twitter.finagle.Http
   *
   * Http.client.withStack(_.prepend(MyStackModule))
   * }}}
   *
   * From Java:
   * {{{
   * import com.twitter.finagle.Http;
   * import static com.twitter.util.Function.func;
   *
   * Http.client().withStack(func(stack -> stack.prepend(MyStackModule)));
   * }}}
   *
   * @see [[withStack(Stack)]]
   * @see [[transformed]]
   */
  def withStack(
    fn: Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]
  ): StackClient[Req, Rep] =
    withStack(fn(stack))

  /**
   * @see [[withStack]]
   */
  def transformed(t: Stack.Transformer): StackClient[Req, Rep] =
    withStack(t(stack))

  // these are necessary to have the right types from Java
  def withParams(ps: Stack.Params): StackClient[Req, Rep]
  def configured[P: Stack.Param](p: P): StackClient[Req, Rep]
  def configured[P](psp: (P, Stack.Param[P])): StackClient[Req, Rep]
  def configuredParams(params: Stack.Params): StackClient[Req, Rep]
}
