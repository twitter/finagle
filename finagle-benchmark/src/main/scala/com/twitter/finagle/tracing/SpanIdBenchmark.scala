package com.twitter.finagle.tracing

import com.twitter.util.RichU64Long
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.util.Random

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
class SpanIdBenchmark {
  import SpanIdBenchmark._

  @Benchmark
  def timeOldToString64BitSpanId(state: SpanIdState64Bit) {
    import state._
    var i = 0
    while (i < n) {
      new RichU64Long(ids(i).low).toU64HexString
      i += 1
    }
  }

  @Benchmark
  def timeToString64BitSpanId(state: SpanIdState64Bit) {
    import state._
    var i = 0
    while (i < n) {
      ids(i).toString
      i += 1
    }
  }

  @Benchmark
  def timeToString128BitSpanId(state: SpanIdState128Bit) {
    import state._
    var i = 0
    while (i < n) {
      ids(i).toString
      i += 1
    }
  }
}

object SpanIdBenchmark {
  @State(Scope.Benchmark)
  class SpanIdState64Bit {
    val rng = new Random(31415926535897932L)
    val n = 1024
    val ids = Array.fill(n)(SpanId(rng.nextLong()))
  }

  @State(Scope.Benchmark)
  class SpanIdState128Bit {
    val rng = new Random(31415926535897932L)
    val n = 1024
    val ids = Array.fill(n)(SpanId(rng.nextLong(), rng.nextLong()))
  }
}
