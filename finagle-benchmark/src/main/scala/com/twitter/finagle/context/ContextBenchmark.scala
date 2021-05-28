package com.twitter.finagle.context

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.io.Buf
import com.twitter.util.Try
import org.openjdk.jmh.annotations.{Benchmark, Level, Param, Scope, Setup, State}

@State(Scope.Benchmark)
class ContextBenchmark extends StdBenchAnnotations {
  import com.twitter.finagle.context.Contexts.broadcast

  class IntKey(id: String) extends broadcast.Key[Int](id) {
    def marshal(value: Int): Buf = sys.error("Unused for benchmarking")
    def tryUnmarshal(buf: Buf): Try[Int] = sys.error("Unused for benchmarking")
  }

  val unusedKey = new IntKey("unused")
  val getKey = new IntKey("getKey")
  val shadowedKey = new IntKey("shadowedKey")

  // 5 is roughly the size of a typical context found in the wild.
  @Param(Array("5"))
  var depth: Int = 0

  var env: Map[String, broadcast.Cell] = _

  // Build up a context with some keys that won't be used
  @Setup(Level.Iteration)
  def setup(): Unit = {

    val pairs = (0 until depth).map { i =>
      val k = new IntKey(s"synthetic-$i")
      broadcast.KeyValuePair(k, 0)
    } :+ broadcast.KeyValuePair(shadowedKey, 10)

    env = broadcast.let(pairs) {
      broadcast.env
    }
  }

  // Shadows one key
  @Benchmark
  def letShadow(): Int = doInContext {
    broadcast.let(shadowedKey, 10) {
      Int.MaxValue
    }
  }

  // Clears one key
  @Benchmark
  def letClearWithKey(): Int = doInContext {
    broadcast.letClear(shadowedKey) {
      Int.MaxValue
    }
  }

  @Benchmark
  def get(): Boolean = doInContext {
    broadcast.get(unusedKey).isEmpty &&
    broadcast.get(shadowedKey).isDefined
  }

  def doInContext[T](fn: => T): T = {
    broadcast.letLocal(env)(fn)
  }
}
