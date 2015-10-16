package com.twitter.finagle.util

import java.net.SocketAddress

import com.twitter.util.{Monitor, NullMonitor}

@deprecated("Use com.twitter.util.RootMonitor instead", "2015-10-15")
object DefaultMonitor extends Monitor {
  def handle(exc: Throwable) = false
  override def toString = "DefaultMonitor"

  val get = this
}

trait ReporterFactory extends ((String, Option[SocketAddress]) => Monitor)

object NullReporterFactory extends ReporterFactory {
  def apply(name: String, addr: Option[SocketAddress]): Monitor = NullMonitor

  override def toString = "NullReporterFactory"
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
