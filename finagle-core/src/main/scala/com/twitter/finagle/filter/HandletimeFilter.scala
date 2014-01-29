package com.twitter.finagle.filter

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Stopwatch, Future}

/**
 * A [[com.twitter.finagle.Filter]] that records the elapsed execution times of
 * the underlying [[com.twitter.finagle.Service]]. Durations are recorded in
 * microseconds and emitted as a stat labeled "handletime_us" to the argument
 * [[com.twitter.finagle.stats.StatsReceiver]].
 */
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
