package com.twitter.finagle.factory

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory}
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Future, Timer}
import java.util.Random
import java.util.concurrent.atomic.AtomicInteger
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

// ko todo: run with some concurrency
@State(Scope.Benchmark)
class ServiceFactoryCacheBenchmark extends StdBenchAnnotations {
  import ServiceFactoryCacheBenchmark._

  @Benchmark
  def apply(state: CacheState): Future[Service[Int, Int]] =
    state.cache(state.nextKey(), state.conn)

}

object ServiceFactoryCacheBenchmark {

  private[this] val newFactory: Int => ServiceFactory[Int, Int] = key => {
    val svc = Service.mk[Int, Int] { in => Future.value(key + in) }
    ServiceFactory.const(svc)
  }

  // use 12 inputs into a cache of size 8 so as to get some misses
  private[this] val MaxCacheSize = 8
  private[this] val MaxInput = 12

  @State(Scope.Benchmark)
  class CacheState {
    val conn: ClientConnection = ClientConnection.nil

    val cache =
      new ServiceFactoryCache[Int, Int, Int](newFactory, Timer.Nil, NullStatsReceiver, MaxCacheSize)

    def nextKey(): Int = {
      val offset = pos.incrementAndGet()
      inputs(offset % inputs.length)
    }

    private[this] val pos: AtomicInteger = new AtomicInteger(0)

    private[this] val inputs: Array[Int] = {
      val rng = new Random(12345L)
      Array.fill(1024) { rng.nextInt(MaxInput) }
    }
  }

}
