package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle._
import com.twitter.finagle.pool._
import com.twitter.util.{Await, Duration, JavaTimer}

// A simple benchmark for pools that measures only get/put times for
// when the pool is full (common case).
class PoolsBenchmark extends SimpleBenchmark {
  type F = ServiceFactory[Int, Int]
  type S = Service[Int, Int]

  val service = new Service[Int, Int] {
    def apply(i: Int) = throw new Exception("you weren't supposed to do that!")
  }
  val underlying = ServiceFactory.const(service)

  def go(pool: F, width: Int, n: Int) {
    var i = 0
    val s = new Array[S](width)
    while (i < n) {
      if (i >= width)
        s(i%width).close()

      s(i%width) = Await.result(pool())

      i += 1
    }
  }

  val width = 100
  private[this] val timer = new JavaTimer

  def timeWatermarkPool(nreps: Int) {
    val p = new WatermarkPool(underlying, 1, width*2)
    go(p, width, nreps)
  }

  def timeCachingPool(nreps: Int) {
    val p = new CachingPool(underlying, width*2, Duration.Top, timer)
    go(p, width, nreps)
  }

  def timeBufferingPool(nreps: Int) {
    val p = new BufferingPool(underlying, width*2)
    go(p, width, nreps)
  }
}
