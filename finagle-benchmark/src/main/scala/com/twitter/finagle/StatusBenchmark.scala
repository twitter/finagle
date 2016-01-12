package com.twitter.finagle

import com.twitter.finagle.Status._
import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
class StatusBenchmark extends StdBenchAnnotations {

  private[this] val all =
    Array(Open, Busy, Closed)

  val combinations: Array[(Status, Status)] =
    for (l <- all; r <- all) yield (l, r)

  @Benchmark
  def best(hole: Blackhole): Unit = {
    var i = 0
    while (i < combinations.length) {
      val statuses = combinations(i)
      hole.consume(Status.best(statuses._1, statuses._2))
      i += 1
    }
  }

  @Benchmark
  def worst(hole: Blackhole): Unit = {
    var i = 0
    while (i < combinations.length) {
      val statuses = combinations(i)
      hole.consume(Status.worst(statuses._1, statuses._2))
      i += 1
    }
  }

}
