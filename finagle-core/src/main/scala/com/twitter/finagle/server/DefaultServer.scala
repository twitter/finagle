package com.twitter.finagle.server

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle._
import com.twitter.finagle.builder.SourceTrackingMonitor
import com.twitter.finagle.filter.{HandletimeFilter, MaskCancelFilter,
  MkJvmFilter, MonitorFilter, RequestSemaphoreFilter}
import com.twitter.finagle.service.{TimeoutFilter, StatsFilter}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, DefaultStatsReceiver}
import com.twitter.finagle.tracing.{Tracer, TracingFilter, DefaultTracer}
import com.twitter.finagle.util.{DefaultMonitor, DefaultTimer, DefaultLogger}
import com.twitter.jvm.Jvm
import com.twitter.util.{Duration, Monitor, Timer}
import java.net.SocketAddress

object DefaultServer {
  private lazy val newJvmFilter = new MkJvmFilter(Jvm())

  /**
   * Configuration for a `DefaultServer`
   *
   * @param requestTimeout The maximum amount of time the server is
   * allowed to handle a request. If the timeout expires, the server
   * will cancel the future and terminate the client connection.
   *
   * @param maxConcurrentRequests The maximum number of concurrent
   * requests the server is willing to handle.
   *
   * @param cancelOnHangup The maximum amount of time the server is
   * allowed to handle a request. If the timeout expires, the server
   * will cancel the future and terminate the client connection.
   *
   * @param prepare Prepare the given `ServiceFactory` before use.
   */
  case class Config[Req, Rep](
    underlying: Server[Req, Rep],
    requestTimeout: Duration = Duration.MaxValue,
     // phase this out. should be handled by the server itself.  how
     // can we expose queue disciplines to the app?
    maxConcurrentRequests: Int = Int.MaxValue,
    cancelOnHangup: Boolean = true,
    prepare: Transformer[Req, Rep] = (sf: ServiceFactory[Req, Rep]) => sf,
    timer: Timer = DefaultTimer.twitter,
    monitor: Monitor = DefaultMonitor,
    logger: java.util.logging.Logger = DefaultLogger,
    statsReceiver: StatsReceiver = DefaultStatsReceiver,
    tracer: Tracer = DefaultTracer
  )

  def apply[Req, Rep](config: Config[Req, Rep]): Server[Req, Rep] =
    new DefaultServer(config)
}

/**
 * A DefaultServer composes on top of an underlying server in order
 * to add standard behavior (like a queueing discipline and
 * cancellation propagation) as well as observability hooks (stats,
 * monitors, tracers).
 */
class DefaultServer[Req, Rep] protected(config: DefaultServer.Config[Req, Rep])
  extends Server[Req, Rep]
{
  import DefaultServer._
  import config._

  protected val outer: Transformer[Req, Rep] = {
    val handletimeFilter = new HandletimeFilter[Req, Rep](statsReceiver)
    val monitorFilter = new MonitorFilter[Req, Rep](monitor andThen new SourceTrackingMonitor(logger, "server"))
    val tracingFilter = new TracingFilter[Req, Rep](tracer)
    val jvmFilter= newJvmFilter[Req, Rep]()

    val filter = handletimeFilter andThen // to measure total handle time
      monitorFilter andThen // to maximize surface area for exception handling
      tracingFilter andThen // to prepare tracing prior to codec tracing support
      jvmFilter  // to maximize surface area

    filter andThen _
  }

  protected val inner: Transformer[Req, Rep] = {
    val maskCancelFilter: SimpleFilter[Req, Rep] =
      if (cancelOnHangup) Filter.identity
      else new MaskCancelFilter

    val statsFilter: SimpleFilter[Req, Rep] =
      if (statsReceiver ne NullStatsReceiver) new StatsFilter(statsReceiver)
      else Filter.identity

    val timeoutFilter: SimpleFilter[Req, Rep] =
      if (requestTimeout < Duration.MaxValue) {
        val exc = new IndividualRequestTimeoutException(requestTimeout)
        new TimeoutFilter(requestTimeout, exc, timer)
      } else Filter.identity

    val requestSemaphoreFilter: SimpleFilter[Req, Rep] =
      if (maxConcurrentRequests == Int.MaxValue) Filter.identity else {
        val sem = new AsyncSemaphore(maxConcurrentRequests)
        new RequestSemaphoreFilter[Req, Rep](sem) {
          // We capture the gauges inside of here so their
          // (reference) lifetime is tied to that of the filter
          // itself.
          val g0 = statsReceiver.addGauge("request_concurrency") {
            maxConcurrentRequests - sem.numPermitsAvailable
          }
          val g1 = statsReceiver.addGauge("request_queue_size") { sem.numWaiters }
        }
      }

    val filter = maskCancelFilter andThen
      requestSemaphoreFilter andThen
      statsFilter andThen
      timeoutFilter

    filter andThen _
  }

  protected val newStack: Transformer[Req, Rep] =
    outer compose prepare compose inner

  def serve(addr: SocketAddress, service: ServiceFactory[Req, Rep]): ListeningServer =
    underlying.serve(addr, newStack(service))
}
