package com.twitter.finagle.util

import java.util.Random
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Duration
import com.twitter.util.NullTimer

object WindowedPercentileHistogramBenchmark {
  val NUM_BUCKETS = 10
  val BUCKET_SIZE: Duration = Duration.fromSeconds(3)
  val VALUES_PER_BUCKET = 10000

  private def fillBucket(wp: WindowedPercentileHistogram, rng: Random): Unit = {
    for (i <- 1 to VALUES_PER_BUCKET) {
      val rand = rng.nextInt(i)
      wp.add(rand)
    }
  }

  @State(Scope.Benchmark)
  class WindowedPercentileState { // Use the Null timer so we can manually force the buckets to flush
    final val wp =
      new WindowedPercentileHistogram(NUM_BUCKETS, BUCKET_SIZE, new NullTimer)
    final val rng = new Random(31415926535897932L)

    /**
     * Set up the test
     */ @Setup(Level.Trial) def setup(): Unit = {
      for (i <- 0 until NUM_BUCKETS) {
        fillBucket(wp, rng)
        wp.flushCurrentBucket()
      }
    }
  }
}
class WindowedPercentileHistogramBenchmark extends StdBenchAnnotations {

  import WindowedPercentileHistogramBenchmark._

  @Benchmark
  def percentile(state: WindowedPercentileState): Int =
    state.wp.percentile(0.50)

  @Benchmark
  def update(state: WindowedPercentileState): Unit = {
    state.wp.flushCurrentBucket()
  }

  @Benchmark
  def add(state: WindowedPercentileState): Unit =
    state.wp.add(state.rng.nextInt(1000))
}
