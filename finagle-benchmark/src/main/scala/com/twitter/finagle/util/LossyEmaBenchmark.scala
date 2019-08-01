package com.twitter.finagle.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Stopwatch
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class LossyEmaBenchmark extends StdBenchAnnotations {

  private[this] val rng: Rng = Rng.threadLocal
  private[this] val ema = new LossyEma(1000, Stopwatch.timeNanos, 0.0)

  @Benchmark
  def update(): Double = {
    ema.update(rng.nextDouble())
  }

}
