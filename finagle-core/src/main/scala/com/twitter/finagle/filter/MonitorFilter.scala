package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.util.{DefaultMonitor, ReporterFactory, LoadedReporterFactory}
import com.twitter.util.{Monitor, Future}

private[finagle] object MonitorFilter {
  object Monitoring extends Stack.Role

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.MonitorFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](Monitoring) {
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val param.Monitor(monitor) = params[param.Monitor]
        new MonitorFilter(monitor) andThen next
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that handles exceptions (incl. raw) thrown
 * by the subsequent [[com.twitter.finagle.Service]]. Exceptions are handled
 * according to the argument [[com.twitter.util.Monitor]].
 */
class MonitorFilter[Req, Rep](monitor: Monitor) extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    Future.monitored {
      service(request)
    } onFailure { exc =>
      monitor.handle(exc)
    }
}
