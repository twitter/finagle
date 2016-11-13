package com.twitter.finagle.util

import com.twitter.finagle.context.RemoteInfo
import com.twitter.logging.{HasLogLevel, Level, Logger}
import com.twitter.util.{Monitor, NullMonitor}
import java.net.SocketAddress
import scala.util.control.NonFatal

private[finagle] object DefaultMonitor {

  /**
   * A minimal logging level above which a default monitor keeps silence.
   */
  val MinLogLevel: Int = Level.INFO.value

  /**
   * A default logger used in default monitor.
   */
  val Log: Logger = Logger(classOf[DefaultMonitor])

  /**
   * Creates a default monitor with default logger.
   */
  def apply(label: String, downstreamAddr: String): DefaultMonitor =
    new DefaultMonitor(Log, label, downstreamAddr)
}

/**
 * The default [[Monitor]] to be used throughout Finagle.
 *
 * This monitor handles exceptions by logging them. Depending on the exception
 * type, different log levels are used:
 *
 *  - [[com.twitter.util.TimeoutException timeout exceptions]] logged with `TRACE`
 *  - [[HasLogLevel exceptions with log level]] logged with their level only if it's below `INFO`
 *  - any other exception is logged as `FATAL`
 *
 * In addition to the stack trace, this monitor also logs upstream socket address, downstream
 * socket address, and a client/server label.
 *
 * @note We refer to "downstream" as a machine/server your clients talks to. We refer to "upstream"
 *       as a client that talks to your machine/server.
 *
 * @note This monitor does not handle (i.e., returns `false`) [[NonFatal fatal exceptions]].
 */
private[util] class DefaultMonitor(
    log: Logger,
    label: String,
    downstreamAddr: String)
  extends Monitor {

  private[this] def upstreamAddr: String =
    RemoteInfo.Upstream.addr.map(_.toString).getOrElse("n/a")

  private[this] def remoteInfo: String =
    s"(upstream address: $upstreamAddr, downstream address: $downstreamAddr, label: $label)"

  private[this] def logWithRemoteInfo(t: Throwable, level: Level): Unit =
    log.logLazy(level, t, s"Exception propagated to the default monitor $remoteInfo.")

  def handle(exc: Throwable): Boolean = {
    exc match {
      case f: HasLogLevel if f.logLevel.value < DefaultMonitor.MinLogLevel =>
        logWithRemoteInfo(exc, f.logLevel)
        true
      case _: com.twitter.util.TimeoutException =>
        // This is a bit convoluted. `Future.within` and `Future.raiseWithin`
        // use `c.t.u.TimeoutExceptions` and these can propagate to the Monitor
        // which in turn leads to noisy logs. By turning the log level down we
        // risk losing other usage of this exception, but it seems like the good
        // outweighs the bad in this case.
        logWithRemoteInfo(exc, Level.TRACE)
        true
      case _ =>
        logWithRemoteInfo(exc, Level.FATAL)
        // We only "handle" non-fatal exceptions.
        NonFatal(exc)
    }
  }

  override def toString: String = "DefaultMonitor"
}

trait ReporterFactory extends ((String, Option[SocketAddress]) => Monitor)

object NullReporterFactory extends ReporterFactory {
  def apply(name: String, addr: Option[SocketAddress]): Monitor = NullMonitor

  override def toString: String = "NullReporterFactory"
}

object LoadedReporterFactory extends ReporterFactory {
  private[this] val factories = LoadService[ReporterFactory]()

  def apply(name: String, addr: Option[SocketAddress]): Monitor =
    factories.map(_(name, addr)).foldLeft(NullMonitor: Monitor) { (a, m) => a andThen m }

  val get = this

  override def toString: String = {
    val names = factories.map(_.getClass.getName).mkString(",")
    s"LoadedReporterFactory($names)"
  }
}
