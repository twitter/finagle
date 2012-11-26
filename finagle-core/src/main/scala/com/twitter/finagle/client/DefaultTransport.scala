package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.builder.SourceTrackingMonitor
import com.twitter.finagle.filter.MonitorFilter
import com.twitter.finagle.service.FailureAccrualFactory
import com.twitter.finagle.service.{CloseOnReleaseService, ExpiringService, 
  FailFastFactory, StatsFilter, TimeoutFilter}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.{DefaultLogger, DefaultMonitor, DefaultTimer}
import com.twitter.util.{Duration, Timer, Monitor}
import java.net.SocketAddress
import java.util.logging.Logger

trait BuildableTransport[Req, Rep] extends (((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep]) {
  /**
   * Use the given request timeout for this transport. By default, it is
   * set to {{com.twitter.util.Duration.MaxValue}}.
   */
  def withRequestTimeout(duration: Duration): BuildableTransport[Req, Rep]

  /**
   * Use the fail-fast mechanism. This is the default behavior
   */
  def withFailFast(): BuildableTransport[Req, Rep]

  /**
   * Turn off the fail-fast mechanism
   */
  def withoutFailFast(): BuildableTransport[Req, Rep]
  // todo: pool params
}

object DefaultTransport {
  private[DefaultTransport] val defaultFailureAccrual: ServiceFactoryWrapper =
    FailureAccrualFactory.wrapper(5, 5.seconds)(DefaultTimer.twitter)

  case class Config[Req, Rep](
    bind: ((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep],
    maxIdletime: Duration = Duration.MaxValue,
    maxLifetime: Duration = Duration.MaxValue,
    requestTimeout: Duration = Duration.MaxValue,
    failFast: Boolean = true,
    failureAccrual: Transformer[Req, Rep] = { factory: ServiceFactory[Req, Rep] =>
      DefaultTransport.defaultFailureAccrual andThen factory
    },
    newPool: (StatsReceiver) => Transformer[Req, Rep] = NewWatermarkPool(),
    timer: Timer = DefaultTimer.twitter,
    monitor: Monitor = DefaultMonitor,
    logger: Logger = DefaultLogger
  )

  def apply[Req, Rep](config: Config[Req, Rep]): DefaultTransport[Req, Rep] =
    new DefaultTransport(config)
}

/**
 * Furnishes an underyling transporter with a pooled, well-behaved
 * stack. The stack includes failure accrual as well as timeout
 * filters.
 *
 * @param maxIdletime The maximum time for which a `Service` is
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
 * @param newPool Use the given stack segment as a pool.
 *
 * @param newTransport The underlying transport.
 */
class DefaultTransport[Req, Rep] protected(config: DefaultTransport.Config[Req, Rep])
    extends (((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep])
    with BuildableTransport[Req, Rep]
{
  def this(bind: ((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep]) =
    this(DefaultTransport.Config[Req, Rep](bind = bind, newPool = NewWatermarkPool()))

  def withRequestTimeout(duration: Duration) = DefaultTransport(config.copy(requestTimeout = duration))
  def withFailFast() = DefaultTransport(config.copy(failFast = true))
  def withoutFailFast() = DefaultTransport(config.copy(failFast = false))

  def apply(tup: (SocketAddress, StatsReceiver)) = {
    val (addr, statsReceiver) = tup
    import config._

    val lifetimeLimited: Transformer[Req, Rep] = {
      val idle = if (maxIdletime < Duration.MaxValue) Some(maxIdletime) else None
      val life = if (maxLifetime < Duration.MaxValue) Some(maxLifetime) else None

      if (!idle.isDefined && !life.isDefined) identity else {
        factory => factory map { service =>
          val closeOnRelease = new CloseOnReleaseService(service)
          new ExpiringService(closeOnRelease, idle, life, timer, statsReceiver) {
            def onExpire() { closeOnRelease.release() }
          }
        }
      }
    }

    val timeBounded: Transformer[Req, Rep] = {
      if (requestTimeout == Duration.MaxValue) identity else {
        val exception = new IndividualRequestTimeoutException(requestTimeout)
        factory => new TimeoutFilter(requestTimeout, exception, timer) andThen factory
      }
    }

    val fastFailed: Transformer[Req, Rep] =
      if (!failFast) identity else
        factory => new FailFastFactory(factory, statsReceiver.scope("failfast"), timer)

    val observed: Transformer[Req, Rep] = {
      val filter = new StatsFilter[Req, Rep](statsReceiver)
      factory => filter andThen factory
    }

    val monitored: Transformer[Req, Rep] = {
      val sourceMonitor = new SourceTrackingMonitor(logger, "client")
      val filter = new MonitorFilter[Req, Rep](monitor andThen sourceMonitor)
      factory => filter andThen factory
    }

    val stack: ((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep] =
      monitored compose
      observed compose
      failureAccrual compose
      timeBounded compose
      newPool(statsReceiver) compose
      fastFailed compose
      lifetimeLimited compose
      bind

    stack(tup)
  }
}
