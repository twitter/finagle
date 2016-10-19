package com.twitter.finagle.netty4

import com.twitter.concurrent.Once
import com.twitter.finagle.stats.{FinagleStatsReceiver, Gauge}
import io.netty.buffer.{PoolArenaMetric, PooledByteBufAllocator}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Exports a number of N4-related metrics under `finagle/netty4`.
 */
private[netty4] object exportNetty4Metrics {

  private[this] val stats = FinagleStatsReceiver.scope("netty4")

  private[this] val gauges = mutable.Set.empty[Gauge]

  private[this] def buildAccumulator(f: PoolArenaMetric => Long) =
    { (acc: Float, pa: PoolArenaMetric) => acc + f(pa) }

  private[this] val sumHugeAllocations = buildAccumulator(_.numHugeAllocations())
  private[this] val sumNormalAllocations = buildAccumulator(_.numNormalAllocations())
  private[this] val sumSmallAllocations = buildAccumulator(_.numSmallAllocations())
  private[this] val sumTinyAllocations = buildAccumulator(_.numTinyAllocations())

  private[this] val sumHugeDeallocations = buildAccumulator(_.numHugeDeallocations())
  private[this] val sumNormalDellocations = buildAccumulator(_.numNormalDeallocations())
  private[this] val sumSmallDeallocations = buildAccumulator(_.numSmallDeallocations())
  private[this] val sumTinyDeallocations = buildAccumulator(_.numTinyDeallocations())

  private[this] val applyOnce: () => Unit = Once {

    if (poolReceiveBuffers()) {
      val allocator = PooledByteBufAllocator.DEFAULT
      val poolingStats = stats.scope("pooling")

      // Allocations.

      gauges.add(poolingStats.addGauge("allocations", "huge")(
        allocator.directArenas().asScala.foldLeft(0.0f)(sumHugeAllocations)
      ))

      gauges.add(poolingStats.addGauge("allocations", "normal")(
        allocator.directArenas().asScala.foldLeft(0.0f)(sumNormalAllocations)
      ))

      gauges.add(poolingStats.addGauge("allocations", "small")(
        allocator.directArenas().asScala.foldLeft(0.0f)(sumSmallAllocations)
      ))

      gauges.add(poolingStats.addGauge("allocations", "tiny")(
        allocator.directArenas().asScala.foldLeft(0.0f)(sumTinyAllocations)
      ))

      // Deallocations.

      gauges.add(poolingStats.addGauge("deallocations", "huge")(
        allocator.directArenas().asScala.foldLeft(0.0f)(sumHugeDeallocations)
      ))

      gauges.add(poolingStats.addGauge("deallocations", "normal")(
        allocator.directArenas().asScala.foldLeft(0.0f)(sumNormalDellocations)
      ))

      gauges.add(poolingStats.addGauge("deallocations", "small")(
        allocator.directArenas().asScala.foldLeft(0.0f)(sumSmallDeallocations)
      ))

      gauges.add(poolingStats.addGauge("deallocations", "tiny")(
        allocator.directArenas().asScala.foldLeft(0.0f)(sumTinyDeallocations)
      ))
    }
  }

  /**
   * Exports N4 metrics.
   *
   * @note This method is thread-safe and no matter how many times it's called,
   *       the metrics will only be exported once.
   */
  def apply(): Unit = applyOnce()
}
