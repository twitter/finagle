package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.factory.{
  BindingFactory, NamerTracingFilter, RefcountedFactory, StatsFactoryWrapper, TimeoutFactory}
import com.twitter.finagle.filter.{DtabStatsFilter, ExceptionSourceFilter, MonitorFilter}
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param._
import com.twitter.finagle.service._
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.ClientStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.Showable
import com.twitter.util.{Future, Var}

object StackClient {
  /**
   * Canonical Roles for each Client-related Stack modules.
   */
  object Role extends Stack.Role("StackClient"){
    val loadBalancer = Stack.Role("LoadBalancer")
    val pool = Stack.Role("Pool")
    val requestDraining = Stack.Role("RequestDraining")
    val prepFactory = Stack.Role("PrepFactory")
    /** PrepConn is special in that it's the first role before the `Endpoint` role */
    val prepConn = Stack.Role("PrepConn")
  }

  /**
   * A [[com.twitter.finagle.Stack]] representing an endpoint.
   * Note that this is terminated by a [[com.twitter.finagle.service.FailingFactory]]:
   * users are expected to terminate it with a concrete service factory.
   *
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
   */
  def endpointStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    // Ensure that we have performed global initialization.
    com.twitter.finagle.Init()

    val stk = new StackBuilder[ServiceFactory[Req, Rep]](nilStack[Req, Rep])
    stk.push(Role.prepConn, identity[ServiceFactory[Req, Rep]](_))
    stk.push(ExpiringService.module)
    stk.push(FailFastFactory.module)
    stk.push(DefaultPool.module)
    stk.push(TimeoutFilter.module)
    stk.push(FailureAccrualFactory.module)
    stk.push(StatsServiceFactory.module)
    stk.push(StatsFilter.module)
    stk.push(DtabStatsFilter.module)
    stk.push(ClientDestTracingFilter.module)
    stk.push(MonitorFilter.module)
    stk.push(ExceptionSourceFilter.module)
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
   * @see [[com.twitter.finagle.factory.RefcountedFactory]]
   * @see [[com.twitter.finagle.factory.TimeoutFactory]]
   * @see [[com.twitter.finagle.factory.StatsFactoryWrapper]]
   * @see [[com.twitter.finagle.tracing.ClientTracingFilter]]
   * @see [[com.twitter.finagle.tracing.TraceInitializerFilter]]
   */
  def newStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    val stk = new StackBuilder(endpointStack[Req, Rep])
    stk.push(LoadBalancerFactory.module)
    stk.push(Role.requestDraining, (fac: ServiceFactory[Req, Rep]) =>
      new RefcountedFactory(fac))
    stk.push(TimeoutFactory.module)
    stk.push(StatsFactoryWrapper.module)
    stk.push(NamerTracingFilter.module)
    stk.push(ClientTracingFilter.module)
    stk.push(Role.prepFactory, identity[ServiceFactory[Req, Rep]](_))
    // The TraceInitializerFilter must be pushed after most other modules so that
    // any Tracing produced by those modules is enclosed in the appropriate
    // span.
    stk.push(TraceInitializerFilter.clientModule)
    stk.result
  }

  /**
   * The default params used for client stacks.
   */
  val defaultParams: Stack.Params = Stack.Params.empty + Stats(ClientStatsReceiver)
}

/**
 * A [[com.twitter.finagle.Client Client]] that composes a
 * [[com.twitter.finagle.Stack Stack]].
 */
trait StackClient[Req, Rep]
    extends Client[Req, Rep]
    with Stack.Parameterized[StackClient[Req, Rep]] {
  /** The current stack. */
  def stack: Stack[ServiceFactory[Req, Rep]]
  /** The current parameter map. */
  def params: Stack.Params
  /** A new StackClient with the provided stack. */
  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): StackClient[Req, Rep]
}

/**
 * The standard template implementation for
 * [[com.twitter.finagle.client.StackClient]].
 *
 */
trait StdStackClient[Req, Rep, This <: StdStackClient[Req, Rep, This]]
    extends StackClient[Req, Rep] { self =>

  protected type In
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
   * Creates a new StackClient with `p` added to the `params`
   * used to configure this StackClient's `stack`.
   */
  def withParams(params: Stack.Params): This =
    copy1(params = params)

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
  protected def endpointer = new Stack.Simple[ServiceFactory[Req, Rep]] {
    val role = Endpoint
    val description = "Send requests over the wire"
    def make(next: ServiceFactory[Req, Rep])(implicit prms: Stack.Params) = {
      val Transporter.EndpointAddr(addr) = get[Transporter.EndpointAddr]
      val endpointClient = copy1(params=prms)
      val transporter = endpointClient.newTransporter()
      ServiceFactory(() => transporter(addr).map(endpointClient.newDispatcher))
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
    val clientParams = params +
      Label(clientLabel) +
      Stats(stats.scope(clientLabel))

    def register(params: Stack.Params) {
      ClientRegistry.register(clientLabel, dest, this.copy1(
        stack = clientStack,
        params = params
      ))
    }

    dest match {
      case Name.Bound(addr) =>
        val clientParams1 = (clientParams +
          LoadBalancerFactory.ErrorLabel(clientLabel) +
          LoadBalancerFactory.Dest(addr))

        register(clientParams1)
        clientStack.make(clientParams1)

      case Name.Path(path) =>
        val BindingFactory.BaseDtab(baseDtab) = params[BindingFactory.BaseDtab]

        val clientParams1 = clientParams + LoadBalancerFactory.ErrorLabel(path.show)

        val vaddr = (baseDtab() orElse Namer.global).bindAndEval(NameTree.Leaf(path))
        // Register this client once as evaluated against the base dtab
        register(clientParams1 + LoadBalancerFactory.Dest(vaddr))

        def newStack(bound: Name.Bound) = {
          clientStack.make(clientParams1 +
            NamerTracingFilter.BoundPath(Some(path, bound)) +
            LoadBalancerFactory.Dest(bound.addr))
        }

        new BindingFactory(path, newStack, baseDtab, stats.scope("interpreter"))
    }
  }
}
