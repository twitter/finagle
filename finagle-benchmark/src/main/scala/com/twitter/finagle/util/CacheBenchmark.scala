package com.twitter.finagle.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.conversions.DurationOps._
import com.twitter.util.NullTimer
import java.util.concurrent.atomic.AtomicLong
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

/**
 * Due to these benchmarks having state in the `cache`, the benchmarks create
 * a fresh cache on every iteration. The benchmark, `baseline`, exists here
 * in order to isolate and factor out those costs. The numbers from that should
 * be subtracted from the other benchmarks to get their real costs.
 */
@State(Scope.Benchmark)
class CacheBenchmark extends StdBenchAnnotations {

  private[this] val cacheSize = 1
  private[this] val ttl = 100.milliseconds
  private[this] val timer = new NullTimer()

  // used for a side-effect on eviction
  private[this] val evictCount = new AtomicLong()
  private[this] val evictor: Option[String => Unit] = Some(_ => evictCount.incrementAndGet())

  private[this] def newCache(): Cache[String] = new Cache[String](
    cacheSize,
    ttl,
    timer,
    evictor
  )

  @Benchmark
  def baseline: Cache[String] = newCache()

  @Benchmark
  def put: Cache[String] = {
    val cache = newCache()
    cache.put("hi")
    cache
  }

  @Benchmark
  def putThenGet: Option[String] = {
    val cache = newCache()
    cache.put("ok")
    cache.get()
  }

  @Benchmark
  def putOverTheLimit: Cache[String] = {
    val cache = newCache()
    cache.put("yep")
    cache.put("over the limit")
    cache
  }

}
