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
import com.twitter.util.{Timer, Duration, Monitor}
import java.net.{SocketAddress, InetSocketAddress}
import java.util.logging.{Level, Logger}

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
  loadBalancerFactory: LoadBalancerFactory = HeapBalancerFactory
) extends Client[Req, Rep] {
  com.twitter.finagle.Init()
  val globalStatsReceiver = new RollupStatsReceiver(statsReceiver)
  private[this] val log = Logger.getLogger(getClass.getName)

  /** Bind a socket address to a well-formed stack */
  val bindStack: SocketAddress => ServiceFactory[Req, Rep] = sa => {
    val hostStats = {
      val host = new RollupStatsReceiver(hostStatsReceiver.scope(
        sa match {
         case ia: InetSocketAddress => "%s:%d".format(ia.getHostName, ia.getPort)
         case other => other.toString
        }))
      BroadcastStatsReceiver(Seq(host, globalStatsReceiver))
    }

    val lifetimeLimited: Transformer[Req, Rep] = {
      val idle = if (maxIdletime < Duration.Top) Some(maxIdletime) else None
      val life = if (maxLifetime < Duration.Top) Some(maxLifetime) else None

      if (!idle.isDefined && !life.isDefined) identity else {
        factory => factory map { service =>
          val closeOnRelease = new CloseOnReleaseService(service)
          new ExpiringService(closeOnRelease, idle, life, timer, hostStats) {
            def onExpire() { closeOnRelease.close() }
          }
        }
      }
    }

    val exceptionSourceFilter: Transformer[Req, Rep] = { factory =>
      new ExceptionSourceFilter[Req, Rep](name) andThen factory
    }

    val timeBounded: Transformer[Req, Rep] = {
      if (requestTimeout == Duration.Top) identity else {
        val exception = new IndividualRequestTimeoutException(requestTimeout)
        factory => new TimeoutFilter(requestTimeout, exception, timer) andThen factory
      }
    }

    val fastFailed: Transformer[Req, Rep] =
      if (!failFast) identity else
        factory => new FailFastFactory(factory, hostStats.scope("failfast"), timer)

    val observed: Transformer[Req, Rep] = {
      val filter = new StatsFilter[Req, Rep](hostStats)
      factory => filter andThen new StatsServiceFactory[Req, Rep](factory, hostStats)
    }

    val monitored: Transformer[Req, Rep] = {
      val filter = new MonitorFilter[Req, Rep](reporter(name, Some(sa)) andThen monitor)
      factory => filter andThen factory
    }

    val traceDest: Transformer[Req, Rep] = {
      val filter = new ClientDestTracingFilter[Req,Rep](sa)
      factory => filter andThen factory
    }

    val newStack: SocketAddress => ServiceFactory[Req, Rep] = exceptionSourceFilter compose
      monitored compose
      traceDest compose
      observed compose
      failureAccrual compose
      timeBounded compose
      pool(hostStats) compose
      fastFailed compose
      lifetimeLimited compose
      (endpointer(_, hostStats))

    newStack(sa)
  }


  val newStack0: Name => ServiceFactory[Req, Rep] = {
    val refcounted: Transformer[Req, Rep] = new RefcountedFactory(_)

    val timeLimited: Transformer[Req, Rep] = factory =>
      if (serviceTimeout == Duration.Top) factory else {
        val exception = new ServiceTimeoutException(serviceTimeout) { serviceName = name }
        new TimeoutFactory(factory, serviceTimeout, exception, timer)
      }

    val traced: Transformer[Req, Rep] = new TracingFilter[Req, Rep](tracer) andThen _

    val observed: Transformer[Req, Rep] = new StatsFactoryWrapper(_, globalStatsReceiver)

    val noBrokersException = new NoBrokersAvailableException(name)

    val balanced: Name => ServiceFactory[Req, Rep] = dest => {
      // TODO: load balancer consumes Var[Addr] directly., 
      // or at least Var[SocketAddress]
      val g = Group.mutable[SocketAddress]()
      dest.bind() observe {
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

      val endpoints = g map { bindStack(_) }
      loadBalancerFactory.newLoadBalancer(
        endpoints, statsReceiver.scope("loadbalancer"), noBrokersException)
    }

    traced compose
      observed compose
      timeLimited compose
      refcounted compose
      balanced
  }

  val newStack: Name => ServiceFactory[Req, Rep] =
    dest => new Refinery(dest, newStack0)

  def newClient(dest: Name, label: String) = copy(
    statsReceiver = statsReceiver.scope(label),
    hostStatsReceiver = hostStatsReceiver.scope(label)
  ).newStack(dest)
}
