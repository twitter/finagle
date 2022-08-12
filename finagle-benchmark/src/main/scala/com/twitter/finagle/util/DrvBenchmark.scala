package com.twitter.finagle.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import scala.util.Random

// ./bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'DrvBenchmark' -prof gc
@State(Scope.Benchmark)
class DrvBenchmark extends StdBenchAnnotations {

  private[this] val seed = 356

  @Param(Array("10", "100", "1000", "10000"))
  var size: Int = _

  var randomNumbers, oneOutlier: Vector[Double] = _

  @Setup
  def setup(): Unit = {
    val r = new Random(seed)
    randomNumbers = Vector.fill(size) { r.nextDouble() }
    oneOutlier = Vector.fill(size - 1) { 1d } :+ 2d
  }

  @Benchmark
  def randomDrv(): Drv = Drv.fromWeights(randomNumbers)

  // Simulates the common case of one instance being performance tested
  @Benchmark
  def oneLargeDrv(): Drv = Drv.fromWeights(oneOutlier)
}
