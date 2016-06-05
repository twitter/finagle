package com.twitter.finagle.memcached.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.memcached.util.ParserUtilsBenchmark.Position
import com.twitter.io.Charsets
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.openjdk.jmh.annotations.{State, Benchmark, Scope}
import scala.util.Random

// ./sbt 'project finagle-benchmark' 'run .*ParserUtilsBenchmark.*'
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

  private val numbers: Seq[ChannelBuffer] = Seq.fill(size) {
    ChannelBuffers.copiedBuffer(rnd.nextInt().toString, Charsets.Utf8)
  }
  private val strings: Seq[ChannelBuffer] = Seq.fill(size) {
    ChannelBuffers.copiedBuffer(rnd.nextString(5), Charsets.Utf8)
  }

  private val _inputs =
    (numbers ++ strings).toIndexedSeq

  @State(Scope.Thread)
  class Position {
    var i = 0

    def inputs: IndexedSeq[ChannelBuffer] = ParserUtilsBenchmark._inputs
  }

}
