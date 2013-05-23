package com.twitter.finagle.server

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle._
import com.twitter.finagle.builder.SourceTrackingMonitor
import com.twitter.finagle.filter.{
  HandletimeFilter, MaskCancelFilter, MkJvmFilter, MonitorFilter, RequestSemaphoreFilter
}
import com.twitter.finagle.service.{TimeoutFilter, StatsFilter}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, ServerStatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.{DefaultMonitor, DefaultTimer, DefaultLogger, ReporterFactory, LoadedReporterFactory}
import com.twitter.jvm.Jvm
import com.twitter.util.{CloseAwaitably, Duration, Monitor, Timer, Closable, Return, Throw, Time}
import java.net.SocketAddress
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

object DefaultServer {
  private val newJvmFilter = new MkJvmFilter(Jvm())
}

/*
 * The default Server implementation. It is given a Listener (eg.
 * [[com.twitter.finagle.netty3.Netty3Listener]]) and a function,
 * serveTransport, that binds a transport and a service. It will then
 * dispatch requests onto a standard service stack parameterized as
 * described below.
 *
 * @param listener The Listener from which to accept new typed
 * Transports.
 *
 * @param serveTransport The function used to bind an accepted
 * Transport with a Service. Requests read from the transport are
 * dispatched onto the Service, with replies written back.
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
case class DefaultServer[Req, Rep, In, Out](
  name: String,
  listener: Listener[In, Out],
  serviceTransport: (Transport[In, Out], Service[Req, Rep]) => Closable,
  requestTimeout: Duration = Duration.Top,
  maxConcurrentRequests: Int = Int.MaxValue,
  cancelOnHangup: Boolean = true,
  prepare: ServiceFactory[Req, Rep] => ServiceFactory[Req, Rep] = (sf: ServiceFactory[Req, Rep]) => sf,
  timer: Timer = DefaultTimer.twitter,
  monitor: Monitor = DefaultMonitor,
  logger: java.util.logging.Logger = DefaultLogger,
  statsReceiver: StatsReceiver = ServerStatsReceiver,
  tracer: Tracer = DefaultTracer,
  reporter: ReporterFactory = LoadedReporterFactory
) extends Server[Req, Rep] {
  com.twitter.finagle.Init()

  private[this] val connections = Collections.newSetFromMap(
    new ConcurrentHashMap[Closable, java.lang.Boolean])

  protected def makeNewStack(statsReceiver: StatsReceiver): Transformer[Req, Rep] = {
    val outer: Transformer[Req, Rep] = {
      val handletimeFilter = new HandletimeFilter[Req, Rep](statsReceiver)
      val monitorFilter = new MonitorFilter[Req, Rep](
        reporter(name, None) andThen monitor andThen new SourceTrackingMonitor(logger, "server"))
      val tracingFilter = new TracingFilter[Req, Rep](tracer)
      val jvmFilter= DefaultServer.newJvmFilter[Req, Rep]()

      val filter = handletimeFilter andThen // to measure total handle time
        monitorFilter andThen // to maximize surface area for exception handling
        tracingFilter andThen // to prepare tracing prior to codec tracing support
        jvmFilter  // to maximize surface area

      filter andThen _
    }

    val inner: Transformer[Req, Rep] = {
      val maskCancelFilter: SimpleFilter[Req, Rep] =
        if (cancelOnHangup) Filter.identity
        else new MaskCancelFilter

      val statsFilter: SimpleFilter[Req, Rep] =
        if (!statsReceiver.isNull) new StatsFilter(statsReceiver)
        else Filter.identity

      val timeoutFilter: SimpleFilter[Req, Rep] =
        if (requestTimeout < Duration.Top) {
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

      val traceDestTransform:Transformer[Req,Rep] = new ServerDestTracingProxy[Req,Rep](_)

      traceDestTransform compose (filter andThen _)
    }

    outer compose prepare compose inner
  }

  def serveTransport(serviceFactory: ServiceFactory[Req, Rep], transport: Transport[In, Out]) {
    val clientConn = new ClientConnection {
      val remoteAddress = transport.remoteAddress
      val localAddress = transport.localAddress
      def close(deadline: Time) = transport.close(deadline)
      val onClose = transport.onClose.map(_ => ())
    }

    serviceFactory(clientConn) respond {
      case Return(service) =>
        val conn = serviceTransport(transport, service)
        connections.add(conn)
        transport.onClose ensure {
          connections.remove(conn)
        }
      case Throw(_) =>
        transport.close()
    }
  }

  def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]): ListeningServer =
    new ListeningServer with CloseAwaitably {
      val scopedStatsReceiver = statsReceiver match {
        case ServerStatsReceiver => statsReceiver.scope(ServerRegistry.nameOf(addr) getOrElse name)
        case sr => sr
      }

      val newStack = makeNewStack(scopedStatsReceiver)

      val underlying = listener.listen(addr) { transport =>
        serveTransport(newStack(factory), transport)
      }

      def closeServer(deadline: Time) = closeAwaitably {
        // The order here is important: by calling underlying.close()
        // first, we guarantee that no further connections are
        // created.
        //
        // TODO: it would be cleaner to fully represent the draining
        // states: accepting no further connections (requests) then
        // fully drained, then closed.
        val closable = Closable.sequence(underlying, Closable.all(connections.asScala.toSeq:_*))
        connections.clear()
        closable.close(deadline)
      }

      def boundAddress = underlying.boundAddress
    }
}
