package com.twitter.finagle.stats

import com.twitter.finagle.benchmark.StdBenchAnnotations
import java.util.Random
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

// ./bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'BucketedHistogramBenchmark'
class BucketedHistogramBenchmark extends StdBenchAnnotations {
  import BucketedHistogramBenchmark._

  @Benchmark
  @OperationsPerInvocation(N)
  def add(data: DataState, add: AddState, bh: Blackhole): Int = {
    var i = 0
    while (i < data.datas.length) {
      bh.consume(add.histogram.add(data.datas(i)))
      i += 1
    }
    i
  }

  @Benchmark
  def percentiles(state: PercentileState): Array[Long] = {
    state.histogram.getQuantiles(BucketedHistogramBenchmark.percentiles)
  }

}

object BucketedHistogramBenchmark {

  final val N = 10000
  final val percentiles = Array[Double](0.5, 0.9, 0.95, 0.99, 0.999, 0.9999)

  @State(Scope.Benchmark)
  class DataState {
    val datas: Array[Long] = {
      val rng = new Random(1010101)
      Array.fill(N) { (rng.nextDouble() * 100000L).toLong }
    }
  }

  @State(Scope.Thread)
  class AddState {
    val histogram = BucketedHistogram()
  }

  @State(Scope.Thread)
  class PercentileState {

    val histogram = BucketedHistogram()

    @Setup
    def setup(state: DataState): Unit = {
      state.datas.foreach(histogram.add)
    }
  }

}
