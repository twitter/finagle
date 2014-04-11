package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.factory._
import com.twitter.finagle.filter.{ExceptionSourceFilter, MonitorFilter}
import com.twitter.finagle.loadbalancer.{LoadBalancerFactory, HeapBalancerFactory}
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{
  BroadcastStatsReceiver, ClientStatsReceiver, NullStatsReceiver, RollupStatsReceiver, 
  StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.{
  DefaultMonitor, DefaultTimer, LoadedReporterFactory, ReporterFactory}
import com.twitter.util.{Duration, Future, Monitor, Return, Timer, Throw, Var}
import java.net.{SocketAddress, InetSocketAddress}
import java.util.logging.{Level, Logger}

private[finagle]
object StackClient {
  type F[Req, Rep] = ServiceFactory[Req, Rep]

  private object noAddr extends SocketAddress {
    override def toString = "noaddr"
  }

  /**
   * Canonical Roles for each Client-related Stack element.
   */
  object Role {
    object ClientStats extends Stack.Role
    object Endpoint extends Stack.Role
    object EndpointTracing extends Stack.Role
    object ExceptionSource extends Stack.Role
    object Expiration extends Stack.Role
    object FactoryStats extends Stack.Role
    object FailFast extends Stack.Role
    object FailureAccrual extends Stack.Role
    object LoadBalancer extends Stack.Role
    object Monitoring extends Stack.Role
    object Netty3 extends Stack.Role
    object RefCounted extends Stack.Role
    object RequestStats extends Stack.Role
    object RequestTimeout extends Stack.Role
    object ServiceTimeout extends Stack.Role
    object Tracing extends Stack.Role

    object Pool extends Stack.Role

    // Introduce a Pool trait in order to form a hierarchy of Pool varieties.
    trait Pool extends Stack.Role
    object BufferingPool extends Role.Pool
    object CachingPool extends Role.Pool
    object ReusingPool extends Role.Pool
    object WatermarkPool extends Role.Pool
  }

  case class Timer(timer: com.twitter.util.Timer)
  implicit object Timer extends Stack.Param[Timer] {
    val default = Timer(DefaultTimer.twitter)
  }

  case class Tracer(tracer: com.twitter.finagle.tracing.Tracer)
  implicit object Tracer extends Stack.Param[Tracer] {
    val default = Tracer(DefaultTracer)
  }

  /**
   * A [[com.twitter.finagle.Stack.Param]] composed of
   * [[com.twitter.finagle.stats.StatsReceiver]]s.
   *
   * {{rawStatsReceiver}} is the raw [[com.twitter.finagle.stats.StatsReceiver]]
   * as passed during client construction. {{statsReceiver}} is a
   * [[com.twitter.finagle.stats.RollupStatsReceiver]] composed of the
   * underlying {{rawStatsReceiver}}.
   */
  class Stats(val rawStatsReceiver: StatsReceiver) {
    val statsReceiver = new RollupStatsReceiver(rawStatsReceiver)
  }
  implicit object Stats extends Stack.Param[Stats] {
    val default = Stats(ClientStatsReceiver)

    def apply(statsReceiver: StatsReceiver) = new Stats(statsReceiver)

    def unapply(stats: Stats): Option[(StatsReceiver, StatsReceiver)] =
      Some((stats.statsReceiver, stats.rawStatsReceiver))
  }

  case class HostStats(hostStatsReceiver: StatsReceiver)
  implicit object HostStats extends Stack.Param[HostStats] {
    val default = HostStats(NullStatsReceiver)
  }

  case class EndpointAddr(addr: SocketAddress)
  implicit object EndpointAddr extends Stack.Param[EndpointAddr] {
    val default = EndpointAddr(noAddr)
  }

  case class Label(label: String)
  implicit object Label extends Stack.Param[Label] {
    val default = Label("")
  }

  case class Logger(log: java.util.logging.Logger)
  implicit object Logger extends Stack.Param[Logger] {
    val default = Logger(java.util.logging.Logger.getLogger("finagle"))
  }

  case class Expiration(idleTime: Duration, lifeTime: Duration)
  implicit object Expiration extends Stack.Param[Expiration] {
    val default = Expiration(Duration.Top, Duration.Top)
  }

  def expiration[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.Expiration) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val Timer(timer) = params[Timer]
      val Expiration(idleTime, lifeTime) = params[Expiration]
      val Stats(statsReceiver, _) = params[Stats]

      if (!idleTime.isFinite && !lifeTime.isFinite)
        return next

      val idle = if (idleTime.isFinite) Some(idleTime) else None
      val life = if (lifeTime.isFinite) Some(lifeTime) else None

      next map { service =>
        val closeOnRelease = new CloseOnReleaseService(service)
        new ExpiringService(closeOnRelease, idle, life, timer, statsReceiver) {
          def onExpire() { closeOnRelease.close() }
        }
      }
    }
  }

  case class FailFast(on: Boolean)
  implicit object FailFast extends Stack.Param[FailFast] {
    val default = FailFast(true)
  }

  def failFast[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.FailFast) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      params[FailFast] match {
        case FailFast(false) => return next
        case _ =>
      }

      val Stats(statsReceiver, _) = params[Stats]
      val Timer(timer) = params[Timer]

      new FailFastFactory(next, statsReceiver.scope(Role.FailFast.toString), timer)
    }
  }

  case class Pool(low: Int, high: Int, bufferSize: Int, idleTime: Duration, maxWaiters: Int)
  implicit object Pool extends Stack.Param[Pool] {
    val default = Pool(0, Int.MaxValue, 0, Duration.Top, Int.MaxValue)
  }

  def pool[Req, Rep] = new Stack.Module[F[Req, Rep]](Role.Pool) {
    import com.twitter.finagle.pool.{CachingPool, WatermarkPool, BufferingPool}

    def make(params: Params, next: Stack[F[Req, Rep]]): Stack[F[Req, Rep]] = {
      val Pool(low, high, bufferSize, idleTime, maxWaiters) = params[Pool]
      val Stats(statsReceiver, _) = params[Stats]
      val Timer(timer) = params[Timer]

      val stack = new StackBuilder[F[Req, Rep]](next)

      if (idleTime > 0.seconds || high > low) {
        stack.push(Role.CachingPool, (sf: ServiceFactory[Req, Rep]) =>
          new CachingPool(sf, high-low, idleTime, timer, statsReceiver)
        )
      }

      stack.push(Role.WatermarkPool, (sf: ServiceFactory[Req, Rep]) =>
        new WatermarkPool(sf, low, high, statsReceiver, maxWaiters)
      )

      if (bufferSize > 0) {
        stack.push(Role.BufferingPool, (sf: ServiceFactory[Req, Rep]) =>
          new BufferingPool(sf, bufferSize)
        )
      }

      stack.result
    }
  }

  case class RequestTimeout(timeout: Duration)
  implicit object RequestTimeout extends Stack.Param[RequestTimeout] {
    val default = RequestTimeout(Duration.Top)
  }
  
  def requestTimeout[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.RequestTimeout) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val RequestTimeout(timeout) = params[RequestTimeout]
      val Timer(timer) = params[Timer]
      if (!timeout.isFinite) return next

      val exc = new IndividualRequestTimeoutException(timeout)
      new TimeoutFilter(timeout, exc, timer) andThen next
    }
  }

  case class FailureAccrual(n: Int, d: Duration)
  implicit object FailureAccrual extends Stack.Param[FailureAccrual] {
    val default = FailureAccrual(5, 5.seconds)
  }
  
  def failureAccrual[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.FailureAccrual) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val FailureAccrual(n, d) = params[FailureAccrual]
      val Timer(timer) = params[Timer]
      FailureAccrualFactory.wrapper(n, d)(timer) andThen next
    }
  }
  
  def factoryStats[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.FactoryStats) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val Stats(statsReceiver, _) = params[Stats]
      if (statsReceiver.isNull) next
      else new StatsServiceFactory(next, statsReceiver)
    }
  }

  def requestStats[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.RequestStats) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val Stats(statsReceiver, _) = params[Stats]
      if (statsReceiver.isNull) next
      else new StatsFilter(statsReceiver) andThen next
    }
  }
  
  def endpointTracing[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.EndpointTracing) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val EndpointAddr(addr) = params[EndpointAddr]
      new ClientDestTracingFilter(addr) andThen next
    }
  }

  case class Monitoring(monitor: Monitor, reporter: ReporterFactory)
  implicit object Monitoring extends Stack.Param[Monitoring] {
    val default = Monitoring(DefaultMonitor, LoadedReporterFactory)
  }
  
  def monitoring[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.Monitoring) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val Monitoring(monitor, reporter) = params[Monitoring]
      val Label(label) = params[Label]
      val EndpointAddr(addr) = params[EndpointAddr]

      val composite = reporter(label, Some(addr)) andThen monitor
      new MonitorFilter(composite) andThen next
    }
  }
  
  def exceptionSource[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.ExceptionSource) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val Label(label) = params[Label]
      new ExceptionSourceFilter(label) andThen next
    }
  }

  def nilStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] =
    new Stack.Leaf(
      Role.Endpoint,
      new FailingFactory[Req, Rep](new IllegalArgumentException("Unterminated stack"))
    )

  /**
   * A stack representing an endpoint. Note that this is terminated
   * by a [[com.twitter.finagle.service.FailingFactory FailingFactory]]:
   * users are expected to terminate it with a concrete service factory.
   */
  def endpointStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    // Ensure that we have performed global initialization.
    com.twitter.finagle.Init()

    val stack = new StackBuilder[ServiceFactory[Req, Rep]](nilStack)

    stack.push(expiration)
    stack.push(failFast)
    stack.push(pool)
    stack.push(requestTimeout)
    stack.push(failureAccrual)
    stack.push(factoryStats)
    stack.push(requestStats)
    stack.push(endpointTracing)
    stack.push(monitoring)
    stack.push(exceptionSource)

    stack.result
  }

  case class LoadBalancer(loadBalancerFactory: LoadBalancerFactory, dest: Var[Addr])
  implicit object LoadBalancer extends Stack.Param[LoadBalancer] {
    val default = LoadBalancer(HeapBalancerFactory, Var.value(Addr.Neg))
  }
  
  def loadBalancer[Req, Rep] = new Stack.Module[F[Req, Rep]](Role.LoadBalancer) {
    def make(params: Params, next: Stack[F[Req, Rep]]): Stack[F[Req, Rep]] = {
      val LoadBalancer(loadBalancerFactory, dest) = params[LoadBalancer]
      val Stats(statsReceiver, rawStatsReceiver) = params[Stats]
      val HostStats(hostStatsReceiver) = params[HostStats]
      val Logger(log) = params[Logger]
      val Label(label) = params[Label]

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

        next.make(params + 
          EndpointAddr(sockaddr) + 
          Stats(stats))
      }

      val balanced = loadBalancerFactory.newLoadBalancer(
        endpoints, rawStatsReceiver.scope(Role.LoadBalancer.toString),
        noBrokersException)

      // observeUntil fails the future on interrupts, but ready
      // should not interruptible DelayedFactory implicitly masks
      // this future--interrupts will not be propagated to it
      val ready = dest.observeUntil(_ != Addr.Pending)
      val f = ready map (_ => balanced)
      
      new Stack.Leaf(Role.LoadBalancer, new DelayedFactory(f))
    }
  }

  case class ServiceTimeout(timeout: Duration)
  implicit object ServiceTimeout extends Stack.Param[ServiceTimeout] {
    val default = ServiceTimeout(Duration.Top)
  }
  
  def serviceTimeout[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.ServiceTimeout) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val ServiceTimeout(timeout) = params[ServiceTimeout]
      val Label(label) = params[Label]
      val Timer(timer) = params[Timer]
      
      val exc = new ServiceTimeoutException(timeout)
      exc.serviceName = label
      new TimeoutFactory(next, timeout, exc, timer)    
    }
  }
  
  def clientStats[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.ClientStats) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val Stats(statsReceiver, _) = params[Stats]
      new StatsFactoryWrapper(next, statsReceiver)
    }
  }
  
  def tracing[Req, Rep] = new Stack.Simple[F[Req, Rep]](Role.Tracing) {
    def make(params: Params, next: F[Req, Rep]): F[Req, Rep] = {
      val Tracer(tracer) = params[Tracer]
      new TracingFilter(tracer) andThen next
    }
  }

  def clientStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = {
    val refCountedTransformer = (sf: ServiceFactory[Req, Rep]) => {
      new RefcountedFactory(sf)
    }

    val stack = new StackBuilder[ServiceFactory[Req, Rep]](endpointStack)
    stack.push(loadBalancer)
    stack.push(serviceTimeout)
    stack.push(Role.RefCounted, refCountedTransformer)
    stack.push(clientStats)
    stack.push(tracing)
    stack.result
  }
}

/**
 * A [[com.twitter.finagle.Stack Stack]]-based client
 */
private[finagle]
class StackClient[Req, Rep](
  val stack: Stack[ServiceFactory[Req, Rep]], 
  params: Stack.Params = Stack.Params.empty)
  extends Client[Req, Rep] {

  def this(endpoint: Stackable[ServiceFactory[Req, Rep]]) =
    this(StackClient.clientStack[Req, Rep] ++ 
      (endpoint +: StackClient.nilStack))

  def configured[P: Stack.Param](p: P): StackClient[Req, Rep] =
    new StackClient(stack, params + p)

  def newClient(dest: Name, label: String): ServiceFactory[Req, Rep] =
    stack.make(params + 
      params[StackClient.LoadBalancer].copy(dest=dest.bind()) +
      StackClient.Label(label))
}

/**
 * A [[com.twitter.finagle.Stack Stack]]-based client which
 * preserves "rich" client semantics.
 */
private[finagle] 
abstract class RichStackClient[Req, Rep, This <: RichStackClient[Req, Rep, This]](
  client: StackClient[Req, Rep]) extends Client[Req, Rep] {
  protected def newRichClient(client: StackClient[Req, Rep]): This
  val stack = client.stack

  def configured[P: Stack.Param](p: P): This =
    newRichClient(client.configured(p))

  def newClient(dest: Name, label: String) = client.newClient(dest, label)
}
