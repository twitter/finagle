package com.twitter.finagle.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@Threads(Threads.MAX)
class ConcurrentRingBufferBench extends StdBenchAnnotations {

  @Param(Array("1000"))
  var size: Int = _

  var b: ConcurrentRingBuffer[Int] = _

  @Setup
  def setup(): Unit = {
    b = new ConcurrentRingBuffer[Int](size)
  }

  @Benchmark
  def timePutAndGet(): Option[Int] = {
    b.tryPut(1)
    b.tryGet()
  }
}
