package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.util.ConcurrentRingBuffer

class ConcurrentRingBufferBenchmark extends SimpleBenchmark {
  val N = 1000
  val b = new ConcurrentRingBuffer[Int](N)

  def timePutAndGet(nreps: Int) {
    var i = 0
    while (i < nreps) {
      b.tryPut(i)
      b.tryGet()
      i += 1
    }
  }
}
