package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Stopwatch
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
@Threads(Threads.MAX)
class LatencyHistogramBench extends StdBenchAnnotations {

  @Param(Array("1000"))
  var maxDurationMs = 1000L

  @Param(Array("-1.0", "0.01", "0.05"))
  var error = 0.0

  var i = 0L

  var histo: LatencyHistogram = _

  @Setup
  def setup(): Unit = {
    val err = if (error <= 0.0) 0.0 else error

    histo = new LatencyHistogram(
      maxDurationMs,
      err,
      1.minute.inMillis,
      LatencyHistogram.DefaultSlices,
      Stopwatch.systemMillis)

    // give it some data to start with
    0L.until(maxDurationMs).foreach(histo.add)
  }

  @Benchmark
  def add(hole: Blackhole): Unit = {
    hole.consume(histo.add(i % maxDurationMs))
    i += 1
  }

  @Benchmark
  def quantile(): Long = {
    histo.quantile(95)
  }

}
