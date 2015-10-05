package com.twitter.finagle.mux

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._
import scala.util.Random

// ./sbt 'project finagle-benchmark' 'run .*WindowedMaxBenchmark.*'
@State(Scope.Benchmark)
class WindowedMaxBenchmark extends StdBenchAnnotations {
  import WindowedMaxBenchmark._

  @Param(Array("100"))
  var windowSize: Int = _

  var windowedMax: WindowedMax = _

  @Setup
  def setup(): Unit = {
    windowedMax = new WindowedMax(windowSize)
  }

  @Benchmark
  def add(s: Stream): Unit = windowedMax.add(s.next())
}

object WindowedMaxBenchmark {
  @State(Scope.Thread)
  class Stream {
    private val input: Array[Long] = Array.fill(1000)(Random.nextLong())
    private var index: Int = 0
    def next(): Long = {
      val n = input(index)
      index = (index + 1) % input.length
      n
    }
  }
}

