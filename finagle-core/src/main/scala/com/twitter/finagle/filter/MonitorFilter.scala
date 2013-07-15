package com.twitter.finagle.filter

import com.twitter.util.{Monitor, Future}

import com.twitter.finagle.{SimpleFilter, Service}

/*
 * A filter that handles all exceptions (incl.  raw) subsequent of
 * this request to the given monitor.
 */
class MonitorFilter[Req, Rep](monitor: Monitor)
  extends SimpleFilter[Req, Rep]
{
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    Future.monitored {
      service(request)
    } onFailure { exc =>
      monitor.handle(exc)
    }
}
