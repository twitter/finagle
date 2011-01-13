package com.twitter.finagle.service

import com.twitter.util.{Future, Time, Throw}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{Service, Filter}

class StatsFilter[Req, Rep](
  statsReceiver: StatsReceiver,
  additionalDescription: (String, String)*)
  extends Filter[Req, Rep, Req, Rep]
{
  private[this] val dispatchSample = statsReceiver.counter(
    (additionalDescription ++ Seq("count" -> "dispatches")):_*)
  private[this] val latencySample = statsReceiver.gauge(
    (additionalDescription ++ Seq("count" -> "latency")):_*)

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val requestedAt = Time.now
    dispatchSample.incr()

    val result = service(request)

    result respond { response =>
      latencySample.measure(requestedAt.untilNow.inMilliseconds)
      response match {
        case Throw(e) =>
          statsReceiver.counter("exception" -> e.getClass.getName).incr()
        case _ => ()
      }
    }

    result
  }
}
