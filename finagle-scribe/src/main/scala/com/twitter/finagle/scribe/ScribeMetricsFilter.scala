package com.twitter.finagle.scribe

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.thrift.scribe.thriftscala.Scribe.Log
import com.twitter.util.Future

private[scribe] class ScribeMetricsFilter(stats: ScribeStats)
    extends SimpleFilter[Log.Args, Log.SuccessType] {
  def apply(
    req: Log.Args,
    svc: Service[Log.Args, Log.SuccessType]
  ): Future[Log.SuccessType] =
    svc(req).respond(stats.respond)
}
