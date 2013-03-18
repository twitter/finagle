package com.twitter.finagle.benchmark

import com.twitter.finagle.exp.LatencyHistogram
import com.google.caliper.SimpleBenchmark
import com.twitter.util.Duration
import com.twitter.conversions.time._

class LatencyHistogramBenchmark extends SimpleBenchmark {
  val N = 500
  var durations: Array[Duration] = _
  val histogram = new LatencyHistogram(Duration.fromMilliseconds(N), 1.second)

  override protected def setUp() {
    durations = Array.range(0, N) map { i => Duration.fromMilliseconds(i) }
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
