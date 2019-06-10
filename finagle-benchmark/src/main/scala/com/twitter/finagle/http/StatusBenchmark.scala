package com.twitter.finagle.http

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class StatusBenchmark extends StdBenchAnnotations {

  @Benchmark
  def fromCode_Cached: Status = {
    Status.fromCode(200)
  }

  @Benchmark
  def fromCode_NotCached: Status = {
    Status.fromCode(600)
  }

}
