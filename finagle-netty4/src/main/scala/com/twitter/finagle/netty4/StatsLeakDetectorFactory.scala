package com.twitter.finagle.netty4

import com.twitter.finagle.stats.StatsReceiver
import io.netty.buffer.ByteBuf
import io.netty.util.{ResourceLeakDetector, ResourceLeakDetectorFactory}

/**
 * `ResourceLeakDetectorFactory` which exports a counter tracking `ByteBuf`
 *  leaks.
 */
private[netty4] class StatsLeakDetectorFactory(stats: StatsReceiver) extends ResourceLeakDetectorFactory {

  private[this] val stashedInstance = ResourceLeakDetectorFactory.instance()

  def newResourceLeakDetector[T](
    resource: Class[T],
    samplingInterval: Int,
    maxActive: Long
  ): ResourceLeakDetector[T] = resource match {
    case x if x.isAssignableFrom(classOf[ByteBuf]) =>
      new LeakDetectorStatsImpl(stats, samplingInterval, maxActive)

    case _ =>
      stashedInstance.newResourceLeakDetector(resource, samplingInterval, maxActive)
  }


  private[this] class LeakDetectorStatsImpl[T](
      sr: StatsReceiver,
      samplingInterval: Int,
      maxActive: Long)
    extends ResourceLeakDetector[T](classOf[ByteBuf], samplingInterval, maxActive) {

    private[this] val referenceLeaks = stats.scope("netty4").counter("reference_leaks")

    protected[this] override def reportTracedLeak(resourceType: String, records: String): Unit = {
      referenceLeaks.incr()
      super.reportTracedLeak(resourceType, records)
    }

    protected[this] override def reportUntracedLeak(resourceType: String): Unit = {
      referenceLeaks.incr()
      super.reportUntracedLeak(resourceType)
    }
  }
}
