package com.twitter.finagle

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.util.AsyncLatch
import org.openjdk.jmh.annotations._

// ./sbt 'project finagle-benchmark' 'jmh:run AsyncLatchBenchmark'
@State(Scope.Benchmark)
class AsyncLatchBenchmark extends StdBenchAnnotations {

  var count = 0

  @Benchmark
  def asyncLatchLifecycle(): Unit = {
    val l = new AsyncLatch(0)
    l.incr()
    l.await { count += 1 }
    l.decr()
  }
}
