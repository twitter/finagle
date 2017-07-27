package com.twitter.finagle.memcached.protocol

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.io.Buf
import org.openjdk.jmh.annotations.{Benchmark, Level, Scope, Setup, State}
import scala.util.Random

@State(Scope.Benchmark)
class KeyValidationBenchmark extends StdBenchAnnotations {

  private[this] val N = 1024

  private[this] val len = 80

  // generate valid keys
  private[this] val keys: Array[Seq[Buf]] = {
    val rnd = new Random(52629004)
    Array.fill(N) {
      val bytes =
        List
          .fill(len) {
            rnd.nextPrintableChar().toByte
          }
          .toArray
      Seq(Buf.ByteArray.Owned(bytes))
    }
  }

  private[this] var i = 0

  @Setup(Level.Iteration)
  def setup(): Unit =
    i = 0

  @Benchmark
  def createGet: Command = {
    i += 1
    Get(keys(i % N))
  }

}
