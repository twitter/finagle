package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.util.DefaultMonitor
import com.twitter.util.{Future, Monitor, Throw, Try}
import scala.util.control.NonFatal

private[finagle] object MonitorFilter {
  val role: Stack.Role = Stack.Role("Monitoring")
  val description: String = "Act as last-resort exception handler"

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.MonitorFilter]]
   * that is intended to use a client.
   */
  def clientModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Monitor, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = MonitorFilter.role
      val description: String = MonitorFilter.description

      def make(m: param.Monitor, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        // We assume that the load balancing factory already prepared a composite
        // monitor (including a `DefaultMonitor`) for us.
        new MonitorFilter(m.monitor).andThen(next)
    }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.MonitorFilter]]
   * that is intended to use a server.
   */
  def serverModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Monitor, param.Label, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = MonitorFilter.role
      val description: String = MonitorFilter.description

      def make(
        _monitor: param.Monitor,
        _label: param.Label,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        // There is no downstream address available on a server so we use "n/a" instead.
        val composite = _monitor.monitor.orElse(DefaultMonitor(_label.label, "n/a"))
        new MonitorFilter(composite).andThen(next)
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that handles exceptions (incl. raw) thrown
 * by the subsequent [[com.twitter.finagle.Service]]. Exceptions are handled
 * according to the argument [[com.twitter.util.Monitor]].
 */
class MonitorFilter[Req, Rep](monitor: Monitor) extends SimpleFilter[Req, Rep] {
  private[this] val someMonitor = Some(monitor)

  private[this] val RespondFn: Try[Rep] => Unit = {
    case Throw(exc) => monitor.handle(exc)
    case _ =>
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    // note: using Monitor's getOption/setOption to avoid allocations
    val saved = Monitor.getOption
    Monitor.setOption(someMonitor)
    try {
      service(request).respond(RespondFn)
    } catch {
      case NonFatal(e) =>
        monitor.handle(e)
        Future.exception(e)
    } finally {
      Monitor.setOption(saved)
    }
  }
}
