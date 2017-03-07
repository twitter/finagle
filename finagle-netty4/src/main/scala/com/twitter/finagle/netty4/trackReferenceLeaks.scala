package com.twitter.finagle.netty4

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.toggle.Toggle
import io.netty.util.{ResourceLeakDetector, ResourceLeakDetectorFactory}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Enable reference leak tracking in netty and export a counter at finagle/netty4/reference_leaks.
 *
 * @note By default samples 1% of buffers but this rate can increased via the
 *       io.netty.leakDetectionLevel env variable.
 *
 *       see: https://netty.io/wiki/reference-counted-objects.html#wiki-h3-11
 */
private[twitter] object trackReferenceLeaks {
  private[this] val underlying: Toggle[Int] =
    Toggles("com.twitter.finagle.netty4.EnableReferenceLeakTracking")

  private[netty4] lazy val enabled: Boolean = underlying(ServerInfo().id.hashCode)

  private[netty4] def leaksDetected(): Int = leakCnt.get()

  private[this] val referenceLeaks = FinagleStatsReceiver.scope("netty4").counter("reference_leaks")

  private[this] val leakCnt: AtomicInteger = new AtomicInteger(0)

  private[netty4] lazy val init: Unit = {
    if (enabled) {
      if (ResourceLeakDetector.getLevel == ResourceLeakDetector.Level.DISABLED)
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.SIMPLE)

      ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(
        new StatsLeakDetectorFactory({ () =>
          referenceLeaks.incr()
          leakCnt.incrementAndGet()
        })
      )
    }
  }
}
