package com.twitter.finagle.util

import com.twitter.finagle.Failure
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

  def handle(exc: Throwable): Boolean = {
    exc match {
      case f: HasLogLevel if f.logLevel.value < MinLogLevel =>
        log.log(f.logLevel, f, "Exception propagated to DefaultMonitor")
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
 * of [[Failure Failures]] with a `Failure.logLevel` below `INFO`.
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
