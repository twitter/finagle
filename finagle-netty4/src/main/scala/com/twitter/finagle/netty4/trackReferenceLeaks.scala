package com.twitter.finagle.netty4

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.toggle.Toggle
import io.netty.util.ResourceLeakDetectorFactory

/**
 * Enable reference leak tracking in netty and export a counter at finagle/netty4/reference_leaks.
 *
 * @note By default samples 1% of buffers but this rate can increased via the
 *       io.netty.leakDetectionLevel env variable.
 *
 *       see: https://netty.io/wiki/reference-counted-objects.html#wiki-h3-11
 */
private[netty4] object trackReferenceLeaks {
  private[this] val underlying: Toggle[Int] =
    Toggles("com.twitter.finagle.netty4.EnableReferenceLeakTracking")

  val enabled = underlying(ServerInfo().id.hashCode)

  lazy val init: Unit = {
    if (enabled) {
      if (System.getProperty("io.netty.leakDetectionLevel", "disabled").equalsIgnoreCase("disabled"))
        System.setProperty("io.netty.leakDetectionLevel", "simple")

      ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(
        new StatsLeakDetectorFactory(FinagleStatsReceiver)
      )
    }
  }
}