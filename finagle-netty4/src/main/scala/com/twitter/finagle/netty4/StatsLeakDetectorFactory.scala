package com.twitter.finagle.netty4

import io.netty.buffer.ByteBuf
import io.netty.util.{ResourceLeakDetector, ResourceLeakDetectorFactory}

/**
 * `ResourceLeakDetectorFactory` which calls `leakFn` on each resource leak.
 */
private[netty4] final class StatsLeakDetectorFactory(
  underlying: ResourceLeakDetectorFactory,
  leakFn: () => Unit)
    extends ResourceLeakDetectorFactory {

  def newResourceLeakDetector[T](
    resource: Class[T],
    samplingInterval: Int,
    maxActive: Long
  ): ResourceLeakDetector[T] = resource match {
    case x if x.isAssignableFrom(classOf[ByteBuf]) =>
      new LeakDetectorStatsImpl(leakFn, samplingInterval, maxActive)

    case _ =>
      underlying.newResourceLeakDetector(resource, samplingInterval, maxActive)
  }

  private[this] class LeakDetectorStatsImpl[T](
    leakFn: () => Unit,
    samplingInterval: Int,
    maxActive: Long)
      extends ResourceLeakDetector[T](classOf[ByteBuf], samplingInterval, maxActive) {

    protected[this] override def reportTracedLeak(resourceType: String, records: String): Unit = {
      leakFn()
      super.reportTracedLeak(resourceType, records)
    }

    protected[this] override def reportUntracedLeak(resourceType: String): Unit = {
      leakFn()
      super.reportUntracedLeak(resourceType)
    }
  }
}
