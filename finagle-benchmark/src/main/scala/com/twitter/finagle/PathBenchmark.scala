package com.twitter.finagle

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class PathBenchmark extends StdBenchAnnotations {

  @Param(Array("/"))
  var path: String = "/"

  @Benchmark
  def read(): Path =
    Path.read(path)

}
