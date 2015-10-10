package com.twitter.finagle.mux

import java.util.concurrent.TimeUnit

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._
import scala.util.Random

// ./sbt 'project finagle-benchmark' 'run .*WindowedMaxBenchmark.*'
@State(Scope.Benchmark)
@Threads(16)
@Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
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
  def addAndGet(s: Stream): Long = {
    windowedMax.add(s.next())
    windowedMax.get
  }

  @Benchmark
  def add(s: Stream): Unit = windowedMax.add(s.next())

  @Benchmark
  def get(): Long = windowedMax.get
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

