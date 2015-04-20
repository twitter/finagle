package com.twitter.finagle.benchmark

import com.twitter.finagle.exp.{LatencyHistogram, WindowedAdder}
import com.google.caliper.SimpleBenchmark
import com.twitter.util.Duration
import com.twitter.conversions.time._

class LatencyHistogramBenchmark extends SimpleBenchmark {
  val N = 500 // in milliseconds
  val durations: Array[Int] = Array.range(0, N)
  val histogram = new LatencyHistogram(N, 1000, WindowedAdder.systemMs)

  override protected def setUp() {
    for (d <- durations)
      histogram.add(d)
  }

  def timeAdd(nreps: Int) {
    var i = 0
    while (i < nreps) {
      histogram.add(durations(i%N))
      i += 1
    }
  }

  def timeQuantile(nreps: Int) {
    var i = 0
    while (i < nreps) {
      histogram.quantile(i%100)
      i += 1
    }
  }
}
