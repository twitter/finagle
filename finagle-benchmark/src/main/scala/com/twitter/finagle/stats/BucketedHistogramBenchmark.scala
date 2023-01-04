package com.twitter.finagle.stats

import com.twitter.finagle.benchmark.StdBenchAnnotations
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

// ./bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'BucketedHistogramBenchmark'
@Warmup(iterations = 2, time = 15, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
class BucketedHistogramBenchmark extends StdBenchAnnotations {
  import BucketedHistogramBenchmark._

  @Benchmark()
  @OperationsPerInvocation(N)
  def add_Tx01(data: DataState, add: AddState, bh: Blackhole): Int = {
    var i = 0
    while (i < data.datas.length) {
      bh.consume(add.histogram.add(data.datas(i)))
      i += 1
    }
    i
  }

  @Benchmark()
  @OperationsPerInvocation(N)
  def addLockFree_Tx01(data: DataState, add: AddState, bh: Blackhole): Int = {
    var i = 0
    while (i < data.datas.length) {
      bh.consume(add.lockFreeHistogram.add(data.datas(i)))
      i += 1
    }
    i
  }

  @Benchmark
  @OperationsPerInvocation(N)
  @Threads(8)
  def add_Tx08(data: DataState, add: AddState, bh: Blackhole): Int = {
    var i = 0
    while (i < data.datas.length) {
      bh.consume(add.histogram.add(data.datas(i)))
      i += 1
    }
    i
  }

  @Benchmark
  @OperationsPerInvocation(N)
  @Threads(8)
  def addLockFree_Tx08(data: DataState, add: AddState, bh: Blackhole): Int = {
    var i = 0
    while (i < data.datas.length) {
      bh.consume(add.lockFreeHistogram.add(data.datas(i)))
      i += 1
    }
    i
  }

  @Benchmark
  @OperationsPerInvocation(N)
  @Threads(16)
  def add_Tx16(data: DataState, add: AddState, bh: Blackhole): Int = {
    var i = 0
    while (i < data.datas.length) {
      bh.consume(add.histogram.add(data.datas(i)))
      i += 1
    }
    i
  }

  @Benchmark
  @OperationsPerInvocation(N)
  @Threads(16)
  def addLockFree_Tx16(data: DataState, add: AddState, bh: Blackhole): Int = {
    var i = 0
    while (i < data.datas.length) {
      bh.consume(add.lockFreeHistogram.add(data.datas(i)))
      i += 1
    }
    i
  }

  @Benchmark
  @Threads(1)
  def percentiles(state: PercentileState): Array[Long] = {
    state.histogram.recompute(state.snapshot)
    state.snapshot.quantiles
  }

  @Benchmark
  @Threads(1)
  def percentilesLockFree(state: PercentileState): Array[Long] = {
    state.lockFreeHistogram.recompute(state.snapshot)
    state.snapshot.quantiles
  }

}

object BucketedHistogramBenchmark {

  final val N = 10000
  final val percentiles = Array[Double](0.5, 0.9, 0.95, 0.99, 0.999, 0.9999)

  @State(Scope.Benchmark)
  class DataState {
    val datas: Array[Long] = {
      val rng = new Random(1010101)
      Array.fill(N) { (rng.nextDouble() * 100000L).toLong }
    }
  }

  @State(Scope.Benchmark)
  class AddState {
    val histogram = BucketedHistogram()
    val lockFreeHistogram = new LockFreeBucketedHistogram()
  }

  @State(Scope.Benchmark)
  class PercentileState {

    val snapshot = new BucketedHistogram.MutableSnapshot(percentiles)
    val histogram = BucketedHistogram()
    val lockFreeHistogram = new LockFreeBucketedHistogram()

    val threads = new Array[Thread](16)
    val stopLatch = new CountDownLatch(1)

    @Setup
    def setup(state: DataState): Unit = {
      state.datas.foreach { x =>
        histogram.add(x)
      }

      val latch = new CountDownLatch(threads.length)
      var i = 0
      while (i < threads.length) {
        threads(i) = new Thread() {
          override def run(): Unit = {
            state.datas.foreach { x =>
              lockFreeHistogram.add(x)
            }
            latch.countDown()
            stopLatch.await()
          }
        }
        threads(i).start()
        i += 1
      }

      latch.await()
    }

    @TearDown
    def tearDown(): Unit = {
      stopLatch.countDown()
    }
  }

}
