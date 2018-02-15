package com.twitter.finagle.tracing

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._
import scala.util.Random

class SpanIdBenchmark extends StdBenchAnnotations {
  import SpanIdBenchmark._

  @Benchmark
  def toString(state: SpanIdState): String = state.nextId().toString

  @Benchmark
  def fromString(state: SpanIdState): Option[SpanId] = SpanId.fromString(state.nextString())
}

object SpanIdBenchmark {
  @State(Scope.Thread)
  class SpanIdState {
    private val rng = new Random(31415926535897932L)
    private var i = 0
    private val ids = Array.fill(1024)(SpanId(rng.nextLong()))
    private val strings = Array.fill(1024)(SpanId(rng.nextLong()).toString)

    def nextId(): SpanId = {
      val j = i % 1024
      i += 1
      ids(j)
    }

    def nextString(): String = {
      val j = i % 1024
      i += 1
      strings(j)
    }
  }
}
