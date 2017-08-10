package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.{Await, Duration, Future, NullMonitor}
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

// ./sbt 'project finagle-benchmark' 'jmh:run MonitorFilterBenchmark'
@State(Scope.Benchmark)
class MonitorFilterBenchmark extends StdBenchAnnotations {

  private[this] val timeout = Duration.Zero

  private[this] val svc: Service[String, String] =
    Service.const(Future.value("mon"))

  private[this] val filter =
    new MonitorFilter[String, String](NullMonitor)

  @Benchmark
  def apply(): String = {
    val res = filter.apply("yeah", svc)
    Await.result(res, timeout)
  }

}
