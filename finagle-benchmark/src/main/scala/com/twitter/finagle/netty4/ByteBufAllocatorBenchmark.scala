package com.twitter.finagle.netty4

import io.netty.buffer.PooledByteBufAllocator
import io.netty.util.ResourceLeakDetector
import io.netty.util.concurrent.DefaultThreadFactory
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

class NettyThreadExecutorService(numThreads: Int, prefix: String)
    extends ThreadPoolExecutor(
      numThreads,
      numThreads,
      0,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable],
      new DefaultThreadFactory("benchmark", true)
    ) {}

@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Threads(5)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class ByteBufAllocatorBenchmark {

  @Setup(Level.Iteration)
  def setup(): Unit = {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED)
  }

  @Param(Array("10240"))
  var allocationSize: Int = 1024 * 10

  @Benchmark
  @Fork(1)
  def allocationBaseline(bh: Blackhole): Unit = {
    val b = PooledByteBufAllocator.DEFAULT.heapBuffer(allocationSize, allocationSize)
    b.release()
    bh.consume(b)
  }

  @Benchmark
  @Fork(
    value = 1,
    jvmArgsPrepend = Array(
      "-Djmh.executor=CUSTOM",
      "-Djmh.executor.class=com.twitter.finagle.netty4.NettyThreadExecutorService"
    )
  )
  def allocationWithFastThreadLocal(bh: Blackhole): Unit = {
    val b = PooledByteBufAllocator.DEFAULT.heapBuffer(allocationSize, allocationSize)
    b.release()
    bh.consume(b)
  }
}
