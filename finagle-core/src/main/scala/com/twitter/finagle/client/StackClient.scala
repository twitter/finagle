package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.factory._
import com.twitter.finagle.filter.{ExceptionSourceFilter, MonitorFilter}
import com.twitter.finagle.loadbalancer.{WeightedLoadBalancerFactory, DefaultBalancerFactory}
import com.twitter.finagle.service._
import com.twitter.finagle.stats._
import com.twitter.finagle.tracing.{ClientDestTracingFilter, TracingFilter}
import com.twitter.util.{Duration, Monitor, Var}
import java.net.{SocketAddress, InetSocketAddress}
import java.util.logging.Level

/**
 * @define param A class eligible for configuring a StackClient's
 * @define module Creates a [[com.twitter.finagle.Stackable]]
 */
private[finagle] object StackClient {
  /**
   * Canonical Roles for each Client-related Stack modules.
   */
  object Role {
    object ClientStats extends Stack.Role
    object EndpointTracing extends Stack.Role
    object LoadBalancer extends Stack.Role
    object Pool extends Stack.Role
    object RefCounted extends Stack.Role
  }

  /**
   * $param endpoint socket address. This is overridden by
   * the `loadBalancer` module.
   */
  case class EndpointAddr(addr: SocketAddress)
  implicit object EndpointAddr extends Stack.Param[EndpointAddr] {
    private[this] val noAddr = new SocketAddress {
      override def toString = "noaddr"
    }
    val default = EndpointAddr(noAddr)
  }

  /**
   * $param per host [[com.twitter.finagle.stats.StatsReceiver]].
   */
  case class HostStats(hostStatsReceiver: StatsReceiver)
  implicit object HostStats extends Stack.Param[HostStats] {
    val default = HostStats(NullStatsReceiver)
  }

  /**
   * $module [[com.twitter.finagle.tracing.ClientDestTracingFilter]].
   */
  def clientDestTracing[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](Role.EndpointTracing) {
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val EndpointAddr(addr) = params[EndpointAddr]
        new ClientDestTracingFilter(addr) andThen next
      }
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
   * @see [[com.twitter.finagle.client.StackClient#clientDestTracing]]
   * @see [[com.twitter.finagle.filter.MonitorFilter]]
   * @see [[com.twitter.finagle.filter.ExceptionSourceFilter]]
   */
  def endpointStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    // Ensure that we have performed global initialization.
    com.twitter.finagle.Init()

    val stk = new StackBuilder[ServiceFactory[Req, Rep]](
      stack.nilStack[Req, Rep])
    stk.push(ExpiringService.module)
    stk.push(FailFastFactory.module)
    stk.push(DefaultPool.module)
    stk.push(TimeoutFilter.module)
    stk.push(FailureAccrualFactory.module)
    stk.push(StatsServiceFactory.module)
    stk.push(StatsFilter.module)
    stk.push(clientDestTracing)
    stk.push(MonitorFilter.module)
    stk.push(ExceptionSourceFilter.module)
    stk.result
  }

  /**
   * $param used to define this StackClient's collection
   * of addresses which will be used to dispatch requests over.
   */
  case class Dest(va: Var[Addr])
  implicit object Dest extends Stack.Param[Dest] {
    val default = Dest(Var.value(Addr.Neg))
  }

  /**
   * $param the load balancing factory used by the `loadBalancer` module.
   */
  case class LoadBalancer(loadBalancerFactory: WeightedLoadBalancerFactory)
  implicit object LoadBalancer extends Stack.Param[LoadBalancer] {
    val default = LoadBalancer(DefaultBalancerFactory)
  }

  /**
   * $module [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]]
   */
  def loadBalancer[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]](Role.LoadBalancer) {
      def make(params: Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val LoadBalancer(loadBalancerFactory) = params[LoadBalancer]
        val Dest(dest) = params[Dest]
        val param.Stats(statsReceiver) = params[param.Stats]
        val HostStats(hostStatsReceiver) = params[HostStats]
        val param.Logger(log) = params[param.Logger]
        val param.Label(label) = params[param.Label]

        val noBrokersException = new NoBrokersAvailableException(label)

        // TODO: load balancer consumes Var[Addr] directly.,
        // or at least Var[SocketAddress]
        val g = Group.mutable[SocketAddress]()
        dest observe {
          case Addr.Bound(sockaddrs) =>
            g() = sockaddrs
          case Addr.Failed(e) =>
            g() = Set()
            log.log(Level.WARNING, "Name binding failure", e)
          case Addr.Delegated(where) =>
            log.log(Level.WARNING,
              "Name was delegated to %s, but delegation is not supported".format(where))
            g() = Set()
          case Addr.Pending =>
            log.log(Level.WARNING, "Name resolution is pending")
            g() = Set()
          case Addr.Neg =>
            log.log(Level.WARNING, "Name resolution is negative")
            g() = Set()
        }

        val endpoints = g map { sockaddr =>
          val stats = if (hostStatsReceiver.isNull) statsReceiver else {
            val scope = sockaddr match {
              case ia: InetSocketAddress =>
                "%s:%d".format(ia.getHostName, ia.getPort)
              case other => other.toString
            }
            val host = new RollupStatsReceiver(hostStatsReceiver.scope(scope))
            BroadcastStatsReceiver(Seq(host, statsReceiver))
          }

          val param.Monitor(monitor) = params[param.Monitor]
          val param.Reporter(reporter) = params[param.Reporter]
          val composite = reporter(label, Some(sockaddr)) andThen monitor

          val endpointStack = next.make(params +
            EndpointAddr(sockaddr) +
            param.Stats(stats) +
            param.Monitor(composite))

          sockaddr match {
            case WeightedSocketAddress(sa, w) => (endpointStack, w)
            case sa => (endpointStack, 1D)
          }
        }

        val rawStatsReceiver = statsReceiver match {
          case sr: RollupStatsReceiver => sr.self
          case sr => sr
        }

        val balanced = loadBalancerFactory.newLoadBalancer(
          endpoints.set, rawStatsReceiver.scope(Role.LoadBalancer.toString),
          noBrokersException)

        // observeUntil fails the future on interrupts, but ready
        // should not interruptible DelayedFactory implicitly masks
        // this future--interrupts will not be propagated to it
        val ready = dest.observeUntil(_ != Addr.Pending)
        val f = ready map (_ => balanced)

        new Stack.Leaf(Role.LoadBalancer, new DelayedFactory(f))
      }
    }

  /**
   * Creates a default finagle client [[com.twitter.finagle.Stack]]. The salient
   * module in the stack is the `loadBalancer`. It creates a new `endpointStack`
   * for each endpoint the client connects to and load balances requests over them.
   * The stack can be configured via [[com.twitter.finagle.Stack.Param]]'s
   * in the finagle package object ([[com.twitter.finagle.param]]) and specific
   * params defined in the companion objects of the respective modules.
   *
   * @see [[com.twitter.finagle.client.StackClient#endpointStack]]
   * @see [[com.twitter.finagle.client.StackClient#loadBalancer]]
   * @see [[com.twitter.finagle.factory.RefCountedFactory]]
   * @see [[com.twitter.finagle.factory.TimeoutFactory]]
   * @see [[com.twitter.finagle.factory.StatsFactoryWrapper]]
   * @see [[com.twitter.finagle.filter.TracingFilter]]
   */
  def newStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    val stk = new StackBuilder(endpointStack[Req, Rep])
    stk.push(loadBalancer)
    stk.push(Role.RefCounted, (fac: ServiceFactory[Req, Rep]) =>
      new RefcountedFactory(fac))
    stk.push(TimeoutFactory.module)
    stk.push(StatsFactoryWrapper.module)
    stk.push(TracingFilter.module)
    stk.result
  }
}

/**
 * A [[com.twitter.finagle.Stack]]-based client.
 */
private[finagle] class StackClient[Req, Rep](
  val stack: Stack[ServiceFactory[Req, Rep]],
  val params: Stack.Params
) extends Client[Req, Rep] {

  /**
   * Creates a default finagle client stack given a stackable
   * `endpoint`. The `endpoint` should represent a protocol
   * implementation which determines concrete `Req` and `Rep`
   * types.
   */
  def this(endpoint: Stackable[ServiceFactory[Req, Rep]]) =
    this(StackClient.newStack[Req, Rep] ++
      (endpoint +: stack.nilStack), Stack.Params.empty)

  /**
   * Creates a new StackClient with `p` added to the `params`
   * used to configure this StackClient's `stack`.
   */
  def configured[P: Stack.Param](p: P): StackClient[Req, Rep] =
    new StackClient(stack, params + p)

  /** @inheritdoc */
  def newClient(dest: Name, label: String): ServiceFactory[Req, Rep] =
    stack.make(params +
      StackClient.Dest(dest.bind()) +
      param.Label(label))
}

/**
 * A [[com.twitter.finagle.Stack Stack]]-based client which
 * preserves "rich" client semantics.
 */
private[finagle]
abstract class RichStackClient[Req, Rep, This <: RichStackClient[Req, Rep, This]](
  client: StackClient[Req, Rep]
) extends Client[Req, Rep] {
  val stack = client.stack

  protected def newRichClient(client: StackClient[Req, Rep]): This

  def configured[P: Stack.Param](p: P): This =
    newRichClient(client.configured(p))

  def newClient(dest: Name, label: String) = client.newClient(dest, label)
}
