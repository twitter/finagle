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
import com.twitter.finagle.stats.ClientStatsReceiver
import com.twitter.finagle.tracing.{ClientDestTracingFilter, TracingFilter}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.Showable
import com.twitter.util.Var

private[finagle] object StackClient {
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
   * @see [[com.twitter.finagle.tracing.TracingFilter]]
   */
  def newStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    val stk = new StackBuilder(endpointStack[Req, Rep])
    stk.push(LoadBalancerFactory.module)
    stk.push(Role.requestDraining, (fac: ServiceFactory[Req, Rep]) =>
      new RefcountedFactory(fac))
    stk.push(TimeoutFactory.module)
    stk.push(StatsFactoryWrapper.module)
    stk.push(TracingFilter.module)
    stk.push(Role.prepFactory, identity[ServiceFactory[Req, Rep]](_))
    stk.result
  }
}

/**
 * A [[com.twitter.finagle.Stack]]-based client.
 */
private[finagle] abstract class StackClient[Req, Rep](
  val stack: Stack[ServiceFactory[Req, Rep]],
  val params: Stack.Params
) extends Client[Req, Rep] { self =>
  protected type In
  protected type Out

   /**
    * A convenient type alias for a client dispatcher.
    */
  protected type Dispatcher = Transport[In, Out] => Service[Req, Rep]

  /**
   * Creates a new StackClient with the default stack (StackClient#newStack)
   * and [[com.twitter.finagle.stats.ClientStatsReceiver]].
   */
  def this() = this(
    StackClient.newStack[Req, Rep],
    Stack.Params.empty + Stats(ClientStatsReceiver)
  )

  /**
   * Defines a typed [[com.twitter.finagle.client.Transporter]] for this client.
   * Concrete StackClient implementations are expected to specify this.
   */
  protected val newTransporter: Stack.Params => Transporter[In, Out]

  /**
   * Defines a dispatcher, a function which reconciles the stream based
   * `Transport` with a Request/Response oriented `Service`.
   * Together with a `Transporter`, it forms the foundation of a
   * finagle client. Concrete implementations are expected to specify this.
   *
   * @see [[com.twitter.finagle.dispatch.GenSerialServerDispatcher]]
   */
  protected val newDispatcher: Stack.Params => Dispatcher

  /**
   * Creates a new StackClient with `f` applied to `stack`.
   */
  def transformed(f: Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]) =
    copy(stack = f(stack))

  /**
   * Creates a new StackClient with `p` added to the `params`
   * used to configure this StackClient's `stack`.
   */
  def configured[P: Stack.Param](p: P): StackClient[Req, Rep] =
    copy(params = params+p)

  /**
   * A copy constructor in lieu of defining StackClient as a
   * case class.
   */
  def copy(
    stack: Stack[ServiceFactory[Req, Rep]] = self.stack,
    params: Stack.Params = self.params
  ): StackClient[Req, Rep] =
    new StackClient[Req, Rep](stack, params) {
      protected type In = self.In
      protected type Out = self.Out
      protected val newTransporter = self.newTransporter
      protected val newDispatcher = self.newDispatcher
      override protected val endpointer = self.endpointer
    }

  /**
   * A stackable module that creates new `Transports` (via transporter)
   * when applied.
   */
  protected val endpointer = new Stack.Simple[ServiceFactory[Req, Rep]] {
    val role = Endpoint
    val description = "Send requests over the wire"
    def make(next: ServiceFactory[Req, Rep])(implicit prms: Stack.Params) = {
      val Transporter.EndpointAddr(addr) = get[Transporter.EndpointAddr]
      val transporter = newTransporter(prms)
      val dispatcher = newDispatcher(prms)
      ServiceFactory(() => transporter(addr) map dispatcher)
    }
  }

  /** @inheritdoc */
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

    def mkServiceFactory(params: Stack.Params): ServiceFactory[Req, Rep] = {
      ClientRegistry.register(clientLabel, this.copy(
        stack = clientStack,
        params = params
      ))
      clientStack.make(params)
    }

    dest match {
      case Name.Bound(addr) =>
        val clientParams1 = (clientParams +
          LoadBalancerFactory.ErrorLabel(clientLabel) +
          LoadBalancerFactory.Dest(addr))

        mkServiceFactory(clientParams1)

      case Name.Path(path) =>
        val clientParams1 = clientParams + LoadBalancerFactory.ErrorLabel(path.show)

        val newStack: Var[Addr] => ServiceFactory[Req, Rep] = { addr =>
          val clientParams2 = clientParams1 + LoadBalancerFactory.Dest(addr)
          mkServiceFactory(clientParams2)
        }

        new BindingFactory(path, newStack, stats.scope("interpreter"))
    }
  }
}

/**
 * A [[com.twitter.finagle.Stack Stack]]-based client which preserves
 * `Like` client semantics. This makes it appropriate for implementing rich
 * clients, since the rich type can be preserved without having to drop down
 * to StackClient[Req, Rep] when making changes.
 */
private[finagle]
abstract class StackClientLike[Req, Rep, Repr <: StackClientLike[Req, Rep, Repr]](
  client: StackClient[Req, Rep]
) extends Client[Req, Rep] {
  val stack = client.stack

  protected def newInstance(client: StackClient[Req, Rep]): Repr

  /**
   * Creates a new `Repr` with an underlying StackClient where `p` has been
   * added to the `params` used to configure this StackClient's `stack`.
   */
  def configured[P: Stack.Param](p: P): Repr =
    newInstance(client.configured(p))

  /**
   * Creates a new `Repr` with an underlying StackClient where `f` has been
   * applied to `stack`.
   */
  protected def transformed(f: Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]): Repr =
    newInstance(client.transformed(f))

  /** @inheritdoc */
  def newClient(dest: Name, label: String) = client.newClient(dest, label)
}
