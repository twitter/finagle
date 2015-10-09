package com.twitter.finagle.pool

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Future, Duration}
import org.openjdk.jmh.annotations._

object PoolBench {
  val underlying = ServiceFactory.const(new Service[Int, Int] {
    def apply(i: Int) = Future.value(0)
  })
}

@Threads(Threads.MAX)
@State(Scope.Benchmark)
class PoolBench extends StdBenchAnnotations {
  import PoolBench._

  @Param(Array("1000"))
  var poolSize: Int = _

  @Param(Array(".999"))
  var loadedRatio: Double = _

  var watermark: ServiceFactory[Int, Int] = _
  var cache: ServiceFactory[Int, Int] = _
  var buffer: ServiceFactory[Int, Int] = _
  var composed: ServiceFactory[Int, Int] = _

  @Setup
  def loadPools() {
    watermark = new WatermarkPool(underlying, lowWatermark = 1, highWatermark = poolSize)
    cache = new CachingPool(underlying, poolSize, Duration.Top, DefaultTimer.twitter)
    buffer = new BufferingPool(underlying, poolSize)
    composed = new WatermarkPool(
      new CachingPool(
        new BufferingPool(underlying, poolSize),
        poolSize,
        Duration.Top,
        DefaultTimer.twitter
      ),
      lowWatermark = 1,
      highWatermark = poolSize
    )

    for (i <- 0 until (poolSize*loadedRatio).toInt) {
      watermark()
      cache()
      buffer()
      composed()
    }
  }

  @Benchmark
  def watermarkGetAndPut(): Unit = Await.result(watermark().flatMap(_.close()))

  @Benchmark
  def cacheGetAndPut(): Unit = Await.result(cache().flatMap(_.close()))

  @Benchmark
  def bufferGetAndPut(): Unit = Await.result(buffer().flatMap(_.close()))

  @Benchmark
  def composedGetAndPut(): Unit = Await.result(composed().flatMap(_.close()))
}

@Threads(Threads.MAX)
@State(Scope.Benchmark)
class SingletonPoolBench extends StdBenchAnnotations {
  import PoolBench._

  val singleton = new SingletonPool(underlying, NullStatsReceiver)

  @Benchmark
  def getAndPut(): Unit = Await.result(singleton().flatMap(_.close()))
}
