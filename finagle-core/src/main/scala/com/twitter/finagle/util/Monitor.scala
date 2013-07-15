package com.twitter.finagle.util

import com.twitter.util.{Monitor, NullMonitor}
import java.net.SocketAddress

// TODO
object DefaultMonitor extends Monitor {
  def handle(exc: Throwable) = false
}

trait ReporterFactory extends ((String, Option[SocketAddress]) => Monitor)

object NullReporterFactory extends ReporterFactory {
  def apply(name: String, addr: Option[SocketAddress]): Monitor = NullMonitor
}

object LoadedReporterFactory extends ReporterFactory {
  private[this] val factories = LoadService[ReporterFactory]()

  def apply(name: String, addr: Option[SocketAddress]): Monitor =
    factories.map(_(name, addr)).foldLeft(NullMonitor: Monitor) { (a, m) => a andThen m }
}
