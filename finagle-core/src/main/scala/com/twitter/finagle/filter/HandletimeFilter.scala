package com.twitter.finagle.filter

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Stopwatch, Future}

class HandletimeFilter[Req, Rep](statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep]
{
  private[this] val stat = statsReceiver.stat("handletime_us")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val elapsed = Stopwatch.start()
    try
      service(request)
    finally
      stat.add(elapsed().inMicroseconds)
  }
}
