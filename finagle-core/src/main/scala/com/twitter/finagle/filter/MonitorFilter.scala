package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.util.{Monitor, Future, Try, Throw}
import scala.util.control.NonFatal

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
  private[this] val RespondFn: Try[Rep] => Unit = {
    case Throw(exc) => monitor.handle(exc)
    case _ =>
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val saved = Monitor.get
    Monitor.set(monitor)
    try {
      service(request).respond(RespondFn)
    } catch {
      case NonFatal(e) =>
        monitor.handle(e)
        Future.exception(e)
    } finally {
      Monitor.set(saved)
    }
  }
}
