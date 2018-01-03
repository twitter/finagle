package com.twitter.finagle.memcached.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.memcached.protocol.Command
import com.twitter.finagle.memcached.protocol.Set
import com.twitter.finagle.memcached.protocol.text.MessageEncoder
import com.twitter.finagle.memcached.protocol.text.client.AbstractCommandToBuf
import com.twitter.finagle.memcached.protocol.text.client.CommandToBuf
import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import com.twitter.util.Time
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import scala.util.Random

class AbstractCommandToBufBenchmark extends StdBenchAnnotations {
  @Benchmark
  def writeDigits(pos: AbstractCommandToBufBenchmark.Position): Unit = {
    val idx = pos.i % pos.inputs.length
    pos.i += 1
    val bw = BufByteWriter.fixed(5)
    AbstractCommandToBuf.writeDigits(pos.inputs(idx), bw)
  }

  @Benchmark
  def lengthAsString(pos: AbstractCommandToBufBenchmark.Position): Int = {
    val idx = pos.i % pos.inputs.length
    pos.i += 1
    AbstractCommandToBuf.lengthAsString(pos.inputs(idx))
  }

  @Benchmark
  def encodeCommand(): Unit = {
    def clientEncoder: MessageEncoder[Command] = new CommandToBuf
    clientEncoder.encode(Set(Buf.Utf8("key"), 0, Time.epoch, Buf.Utf8("value")))
  }
}

object AbstractCommandToBufBenchmark {
  private val size = 100000
  private val rnd = new Random(69230L) // constant seed, to give us consistent values

  private val inputs: Array[Int] = Array.fill(size) {
    rnd.nextInt() % 5000
  }

  @State(Scope.Thread)
  class Position {
    var i = 0
    def inputs: Array[Int] = AbstractCommandToBufBenchmark.inputs
  }
}
