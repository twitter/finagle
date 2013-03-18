package com.twitter.finagle.service

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{BackupRequestLost, SourcedException, Service, SimpleFilter}
import com.twitter.util.{Future, Stopwatch, Throw, Return}
import java.util.concurrent.atomic.AtomicInteger

class StatsFilter[Req, Rep](statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep]
{
  private[this] val outstandingRequestCount = new AtomicInteger(0)
  private[this] val dispatchCount = statsReceiver.counter("requests")
  private[this] val successCount = statsReceiver.counter("success")
  private[this] val latencyStat = statsReceiver.stat("request_latency_ms")
  private[this] val outstandingRequestCountgauge =
    statsReceiver.addGauge("pending") { outstandingRequestCount.get }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val elapsed = Stopwatch.start()

    outstandingRequestCount.incrementAndGet()
    val result = service(request)

    result respond {
      case Throw(BackupRequestLost) =>
        // We blackhole this request. It doesn't count for anything.
        // After the Failure() patch, this should no longer need to
        // be a special case.
        outstandingRequestCount.decrementAndGet()
      case Throw(e) =>
        dispatchCount.incr()
        latencyStat.add(elapsed().inMilliseconds)
        def flatten(ex: Throwable): Seq[String] =
          if (ex eq null) Seq[String]() else ex.getClass.getName +: flatten(ex.getCause)
        statsReceiver.scope("failures").counter(flatten(e): _*).incr()
        e match {
          case sourced: SourcedException if sourced.serviceName != "unspecified" =>
            statsReceiver
              .scope("sourcedfailures")
              .counter(sourced.serviceName +: flatten(sourced): _*)
              .incr()
          case _ =>
        }
      case Return(_) =>
        dispatchCount.incr()
        successCount.incr()
        latencyStat.add(elapsed().inMilliseconds)
    }

    result
  }
}
