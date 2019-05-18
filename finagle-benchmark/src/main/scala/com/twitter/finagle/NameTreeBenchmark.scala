package com.twitter.finagle

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.util.Var
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

@State(Scope.Benchmark)
class NameTreeBenchmark extends StdBenchAnnotations {

  private[this] val path = Path.read("/foo/bar")
  private[this] val bound = Name.Bound(Var.value(Addr.Pending), path)
  private[this] val leaf = NameTree.Leaf(bound)

  @Benchmark
  def leafHashCode: Int =
    leaf.hashCode

}
