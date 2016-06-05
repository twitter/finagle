package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Await, JavaTimer, Future}
import java.util.concurrent.atomic.AtomicInteger
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

/**
 * This test is used for allocation more so than performance.
 *
 * In other words, it's best run with "-prof gc".
 */
@State(Scope.Benchmark)
class BackupRequestFilterBenchmark extends StdBenchAnnotations {

  private[this] val i = new AtomicInteger()
  private[this] val timer = new JavaTimer(true) // we need high resolution
  private[this] val Response = Future.value(1000)

  private[this] val backupReqFilter = new BackupRequestFilter[String, Int](
    95,
    1.second,
    timer,
    NullStatsReceiver,
    1.minute
  )

  private[this] val svc =
    backupReqFilter.andThen(Service.const(Response))

  private[this] val sometimesSleepySvc =
    backupReqFilter.andThen(Service.mk[String, Int] { _ =>
      if (i.incrementAndGet() % 100 == 0)
        Thread.sleep(2)
      Response
    })

  @Benchmark
  def noBackups(): Int =
    Await.result(svc(""))

  @Benchmark
  def onePercentBackups(): Int =
    Await.result(sometimesSleepySvc(""))

}
