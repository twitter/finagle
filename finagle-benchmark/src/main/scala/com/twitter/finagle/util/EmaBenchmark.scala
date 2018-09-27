package com.twitter.finagle.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._

class EmaBenchmark extends StdBenchAnnotations {
  import EmaBenchmark._

  @Benchmark
  def update(state: Synchronized): Double = {
    state.synchronized {
      state.ema.update(state.monotime.nanos(), state.rng.nextDouble())
    }
  }

}

object EmaBenchmark {

  @State(Scope.Benchmark)
  class Synchronized {
    val rng: Rng = Rng.threadLocal
    val monotime = new Ema.Monotime()
    val ema: Ema = new Ema(1000)
  }
}
