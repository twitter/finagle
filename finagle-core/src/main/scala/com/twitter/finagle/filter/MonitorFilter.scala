package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.util.{Monitor, Future}

private[finagle] object MonitorFilter {
  val role = Stack.Role("Monitoring")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.MonitorFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Monitor, ServiceFactory[Req, Rep]]{
      val role = MonitorFilter.role
      val description = "Act as last-resort exception handler"
      def make(_monitor: param.Monitor, next: ServiceFactory[Req, Rep]) = {
        val param.Monitor(monitor) = _monitor
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

  private[this] val OnFailureFn: Throwable => Unit =
    exc => monitor.handle(exc)

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    Future.monitored {
      service(request)
    }.onFailure(OnFailureFn)
}
