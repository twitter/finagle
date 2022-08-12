package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.NullMonitor
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State

// /bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'MonitorFilterBenchmark'
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
