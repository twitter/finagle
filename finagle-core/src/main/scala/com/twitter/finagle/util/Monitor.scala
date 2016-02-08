package com.twitter.finagle.util

import com.twitter.logging.{HasLogLevel, Logger, Level}
import com.twitter.util.{RootMonitor, Monitor, NullMonitor}
import java.net.SocketAddress

/**
 * Exposed for testing.
 *
 * Use the companion object [[DefaultMonitor]].
 */
private[util] class DefaultMonitor(log: Logger) extends Monitor {
  private[this] val MinLogLevel = Level.INFO.value

  private[this] def logThrowable(t: Throwable, level: Level): Unit =
    log.log(level, t, "Exception propagated to DefaultMonitor")

  def handle(exc: Throwable): Boolean = {
    exc match {
      case f: HasLogLevel if f.logLevel.value < MinLogLevel =>
        logThrowable(exc, f.logLevel)
        true
      case _: com.twitter.util.TimeoutException =>
        // This is a bit convoluted. `Future.within` and `Future.raiseWithin`
        // use `c.t.u.TimeoutExceptions` and these can propagate to the Monitor
        // which in turn leads to noisy logs. By turning the log level down we
        // risk losing other usage of this exception, but it seems like the good
        // outweighs the bad in this case.
        logThrowable(exc, Level.TRACE)
        true
      case _ =>
        RootMonitor.handle(exc)
    }
  }

  override def toString: String = "DefaultMonitor"
}

/**
 * The default [[Monitor]] to be used throughout Finagle.
 *
 * Mostly delegates to [[RootMonitor]], with the exception
 * of [[HasLogLevel HasLogLevels]] with a `logLevel` below `INFO`.
 * [[com.twitter.util.TimeoutException TimeoutExceptions]] are also
 * suppressed because they often originate from `Future.within` and
 * `Future.raiseWithin`.
 */
object DefaultMonitor
  extends DefaultMonitor(Logger.get(classOf[DefaultMonitor]))
{
  val get = this
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
