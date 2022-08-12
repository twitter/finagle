package com.twitter.finagle.memcached.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.io.Buf
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import scala.util.Random

// ./bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- ' ParserUtilsBenchmark'
class ParserUtilsBenchmark extends StdBenchAnnotations {
  import ParserUtilsBenchmark._

  @Benchmark
  def isDigits(pos: Position): Boolean = {
    val idx = pos.i % pos.inputs.length
    pos.i += 1
    ParserUtils.isDigits(pos.inputs(idx))
  }

  @Benchmark
  def bufToInt(pos: Position): Int = {
    val idx = pos.i % pos.inputs.length
    pos.i += 1
    val buf = pos.inputs(idx)
    ParserUtils.bufToInt(buf)
  }

}

object ParserUtilsBenchmark {

  private val size = 100000
  private val rnd = new Random(69230L) // just to give us consistent values

  private val inputs: Array[Buf] = Array.fill(size) {
    Buf.Utf8(math.abs(rnd.nextInt()).toString)
  }

  @State(Scope.Thread)
  class Position {
    var i = 0

    def inputs: Array[Buf] = ParserUtilsBenchmark.inputs
  }

}
