package com.twitter.finagle.netty4

import com.twitter.concurrent.Once
import com.twitter.finagle.stats.{FinagleStatsReceiver, Gauge}
import com.twitter.util.registry.GlobalRegistry
import io.netty.buffer.{PoolArenaMetric, PooledByteBufAllocator}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Exports a number of N4-related metrics under `finagle/netty4` and registers
 * static values under `library/netty4` in the Registry.
 */
private[netty4] object exportNetty4MetricsAndRegistryEntries {

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

  private[this] val exportMetrics = Once {

    if (poolReceiveBuffers() || usePooling()) {
      val metric = PooledByteBufAllocator.DEFAULT.metric()
      val poolingStats = stats.scope("pooling")

      // Allocations.

      gauges.add(poolingStats.addGauge("allocations", "huge")(
        metric.directArenas().asScala.foldLeft(0.0f)(sumHugeAllocations)
      ))

      gauges.add(poolingStats.addGauge("allocations", "normal")(
        metric.directArenas().asScala.foldLeft(0.0f)(sumNormalAllocations)
      ))

      gauges.add(poolingStats.addGauge("allocations", "small")(
        metric.directArenas().asScala.foldLeft(0.0f)(sumSmallAllocations)
      ))

      gauges.add(poolingStats.addGauge("allocations", "tiny")(
        metric.directArenas().asScala.foldLeft(0.0f)(sumTinyAllocations)
      ))

      // Deallocations.

      gauges.add(poolingStats.addGauge("deallocations", "huge")(
        metric.directArenas().asScala.foldLeft(0.0f)(sumHugeDeallocations)
      ))

      gauges.add(poolingStats.addGauge("deallocations", "normal")(
        metric.directArenas().asScala.foldLeft(0.0f)(sumNormalDellocations)
      ))

      gauges.add(poolingStats.addGauge("deallocations", "small")(
        metric.directArenas().asScala.foldLeft(0.0f)(sumSmallDeallocations)
      ))

      gauges.add(poolingStats.addGauge("deallocations", "tiny")(
        metric.directArenas().asScala.foldLeft(0.0f)(sumTinyDeallocations)
      ))
    }
  }

  private[this] val exportRegistryEntries = Once {
    if (poolReceiveBuffers() || usePooling()) {
      val metric = PooledByteBufAllocator.DEFAULT.metric()

      GlobalRegistry.get.put(
        Seq("library", "netty4", "pooling", "chunkSize"), metric.chunkSize.toString
      )

      GlobalRegistry.get.put(
        Seq("library", "netty4", "pooling", "numDirectArenas"), metric.numDirectArenas.toString
      )

      GlobalRegistry.get.put(
        Seq("library", "netty4", "pooling", "numHeapArenas"), metric.numHeapArenas.toString
      )
    }

    GlobalRegistry.get.put(
      Seq("library", "netty4", "native epoll enabled"), nativeEpoll.enabled.toString
    )
  }

  /**
   * Exports N4 metrics and registry entries.
   *
   * @note This method is thread-safe and no matter how many times it's called,
   *       the metrics/registries will only be exported once.
   */
  def apply(): Unit = {
    exportMetrics()
    exportRegistryEntries()
  }
}
