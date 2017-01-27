package com.twitter.finagle.memcached.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.memcached.util.ParserUtilsBenchmark.Position
import com.twitter.io.Buf
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}
import scala.util.Random

// ./sbt 'project finagle-benchmark' 'jmh:run ParserUtilsBenchmark'
class ParserUtilsBenchmark extends StdBenchAnnotations {

  @Benchmark
  def isDigits(pos: Position): Boolean = {
    val idx = pos.i % pos.inputs.length
    pos.i += 1
    ParserUtils.isDigits(pos.inputs(idx))
  }

}

object ParserUtilsBenchmark {

  private val size = 100000
  private val rnd = new Random(69230L) // just to give us consistent values

  private val inputs: Array[Buf] = Array.fill(size) {
    Buf.Utf8(rnd.nextInt().toString)
  }

  @State(Scope.Thread)
  class Position {
    var i = 0

    def inputs: Array[Buf] = ParserUtilsBenchmark.inputs
  }

}
