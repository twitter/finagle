package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.factory.{RefcountedFactory, StatsFactoryWrapper,
  TimeoutFactory}
import com.twitter.finagle.loadbalancer.HeapBalancer
import com.twitter.finagle.stats.{DefaultStatsReceiver, NullStatsReceiver,
  RollupStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{DefaultTracer, Tracer, TracingFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Future, Timer, Duration, Monitor}
import java.net.{SocketAddress, InetSocketAddress}

trait BuildableClient[Req, Rep] extends Client[Req, Rep] {
  /**
   * Create a new client with the given service timeout. This is the
   * maximum amount of time allowed for acquiring a service (making a
   * connection). By default, the timeout is
   * {{com.twitter.util.Duration.MaxValue}}.
   */
  def withServiceTimeout(duration: Duration): BuildableClient[Req, Rep]

  /**
   * Report stats to the given StatsReceiver. Stats are reported to
   * the global stats receiver by default.
   */
  def withStatsReceiver(statsReceiver: StatsReceiver): BuildableClient[Req, Rep]

  /**
   * Report traces to the given tracer. By default, traces are
   * reported to the global tracer.
   */
  def withTracer(tracer: Tracer): BuildableClient[Req, Rep]
}

object DefaultClient {
  case class Config[Req, Rep](
    transport: ((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep],
    serviceTimeout: Duration = Duration.MaxValue,
    timer: Timer = DefaultTimer.twitter,
    statsReceiver: StatsReceiver = DefaultStatsReceiver,
    tracer: Tracer  = DefaultTracer,
    name: String = "unknown"
  )

  def apply[Req, Rep](config: Config[Req, Rep]): DefaultClient[Req, Rep] = new DefaultClient(config)
}

/**
 * The default `Client` implementation. Given a transporter,
 * `DefaultClient` creates clients that use a least-loaded load
 * balancer to balanced requests across the connected cluster.
 */
class DefaultClient[Req, Rep] protected(config: DefaultClient.Config[Req, Rep])
    extends Client[Req, Rep]
    with BuildableClient[Req, Rep]
{
  /**
   * @param transport The endpoint transporter. One such endpoint is
   * created for each member of a client's cluster.
   */
  def this(transport: ((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep]) =
    this(DefaultClient.Config(transport))

  def withServiceTimeout(duration: Duration) = DefaultClient(config.copy(serviceTimeout = duration))
  def withStatsReceiver(statsReceiver: StatsReceiver) = DefaultClient(config.copy(statsReceiver = statsReceiver))
  def withTracer(tracer: Tracer) = DefaultClient(config.copy(tracer = tracer))

  import config._
  // Refcounting is required to "rectify" the service lifecycle: it
  // guarantees that a service cannot be released with a pending
  // request. This allows filters below the refcounter to rely on
  // this behavior.
  private val refcounted: Transformer[Req, Rep] = new RefcountedFactory(_)

  val timeBounded: Transformer[Req, Rep] = factory =>
    if (serviceTimeout == Duration.MaxValue) factory else {
      val exception = new ServiceTimeoutException(serviceTimeout)
      new TimeoutFactory(factory, serviceTimeout, exception, timer)
    }

  val traced: Transformer[Req, Rep] = new TracingFilter[Req, Rep](tracer) andThen _
  val observed: Transformer[Req, Rep] = new StatsFactoryWrapper(_, statsReceiver)
  
  val noBrokersException = new NoBrokersAvailableException(name)

  val balanced: Cluster[SocketAddress] => ServiceFactory[Req, Rep] = cluster => {
    val endpoints = cluster map { addr =>
      val hostStatsReceiver = new RollupStatsReceiver(statsReceiver).withSuffix(
        addr match {
         case ia: InetSocketAddress => "%s:%d".format(ia.getHostName, ia.getPort)
         case other => other.toString
        })

      transport(addr, hostStatsReceiver)
    }

    new HeapBalancer(endpoints, statsReceiver.scope("loadbalancer"), noBrokersException)
  }

  val stack: Cluster[SocketAddress] => ServiceFactory[Req, Rep] =
    traced compose
      observed compose
      timeBounded compose
      refcounted compose
      balanced

  def newClient(cluster: Cluster[SocketAddress]) = stack(cluster)
}
