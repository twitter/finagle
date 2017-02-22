package com.twitter.finagle.netty4

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.toggle.Toggle
import com.twitter.util.lint.{Category, Issue, Rule}
import io.netty.util.ResourceLeakDetectorFactory
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

  private[netty4] val enabled = underlying(ServerInfo().id.hashCode)

  private[this] val referenceLeaks = FinagleStatsReceiver.scope("netty4").counter("reference_leaks")

  private[this] val leaksDetected: AtomicInteger = new AtomicInteger(0)

  def leakDetectedRule(): Rule = Rule(Category.Runtime,
    "Reference leak detected",
    "A reference leak was detected, check your log files for leak tracing information."
  ) {
    if (leaksDetected.get() > 0) Seq(Issue(s"${leaksDetected.get()} leaks detected"))
    else Nil
  }

  private[netty4] lazy val init: Unit = {
    if (enabled) {
      if (System.getProperty("io.netty.leakDetectionLevel", "disabled").equalsIgnoreCase("disabled"))
        System.setProperty("io.netty.leakDetectionLevel", "simple")

      ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(
        new StatsLeakDetectorFactory({ () =>
          referenceLeaks.incr()
          leaksDetected.incrementAndGet()
        })
      )
    }
  }
}
