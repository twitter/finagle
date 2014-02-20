package com.twitter.finagle.filter

import com.twitter.util.{Monitor, Future}
import com.twitter.finagle.{SimpleFilter, Service}

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
