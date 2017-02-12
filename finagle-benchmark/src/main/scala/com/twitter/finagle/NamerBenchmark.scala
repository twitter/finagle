package com.twitter.finagle

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Activity
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class NamerBenchmark extends StdBenchAnnotations {

  @Param(Array("/$/nil"))
  var path: String = "/$/nil"

  private[this] var parsedPath: Path = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    val pathParts = path.split('/').drop(1)
    parsedPath = Path.Utf8(pathParts: _*)
  }

  @Benchmark
  def lookup(): Activity[NameTree[Name]] =
    Namer.global.lookup(parsedPath)

}
