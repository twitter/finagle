package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Future, Promise, Return, Throw, Duration, Timer}

/**
 * Issue a backup request after `delay` time has elapsed. This is
 * useful for curtailing tail latencies in distributed systems.
 *
 * '''Note:''' Care must be taken to ensure that application of this
 * filter preserves semantics since a request may be issued twice
 * (ie. they are idempotent).
 */
class BackupRequestFilter[Req, Rep](delay: Duration, timer: Timer, statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep]
{
  assert(delay >= 0.seconds)

  private[this] val backups = statsReceiver.scope("backup")
  private[this] val timeouts = backups.counter("timeouts")
  private[this] val won = backups.counter("won")
  private[this] val lost = backups.counter("lost")

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val backup = timer.doLater(delay) {
      timeouts.incr()
      service(req)
    } flatten

    Future.select(Seq(service(req), backup)) flatMap {
      case (Return(res), Seq(other)) =>
        if (other eq backup) won.incr() else lost.incr()
        other.cancel()
        Future.value(res)
      case (Throw(_), Seq(other)) => other
    }
  }
}
