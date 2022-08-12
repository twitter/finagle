package com.twitter.finagle.context

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

// ./bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'LocalContextBenchmark' -prof gc
@State(Scope.Benchmark)
class LocalContextBenchmark extends StdBenchAnnotations {
  import com.twitter.finagle.context.Contexts.local

  @Param(Array("5"))
  var depth: Int = 0

  var env: Map[Contexts.local.Key[_], Any] = _

  val unusedKey = new local.Key[Any]
  val realKey = new local.Key[Int]

  @Setup(Level.Iteration)
  def setup(): Unit = {

    val pairs = (0 until depth).map { i =>
      val k = new local.Key[Int]
      local.KeyValuePair(k, 0)
    } :+ local.KeyValuePair(realKey, 10)

    env = local.let(pairs) {
      local.env
    }
  }

  @Benchmark
  def let(): Int = doInContext(doLet)

  private val doLet: () => Int = () => {
    local.let(realKey, 15) {
      Int.MaxValue
    }
  }

  @Benchmark
  def get(): Boolean = doInContext(doGet)

  private val doGet: () => Boolean = () => {
    local.get(unusedKey).isEmpty &&
      local.get(realKey).isDefined
  }

  @Benchmark
  def getOrElse(bh: Blackhole): Int = doInContext(doGetOrElse, bh)

  private val doGetOrElse: (Blackhole) => Int = (bh: Blackhole) => {
    bh.consume { local.getOrElse(unusedKey, () => None) }
    local.getOrElse(realKey, () => ???)
  }

  @Benchmark
  def letClear(): Int = doInContext(doLetClear)

  private val doLetClear: () => Int = () => {
    local.letClear(realKey) {
      Int.MaxValue
    }
  }

  def doInContext[T](fn: () => T): T = {
    local.letLocal(env)(fn())
  }

  def doInContext[T](fn: Blackhole => T, bh: Blackhole): T = {
    local.letLocal(env)(fn(bh))
  }
}
