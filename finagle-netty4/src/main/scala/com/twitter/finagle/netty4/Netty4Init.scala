package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag
import com.twitter.finagle.FinagleInit
import com.twitter.finagle.stats.FinagleStatsReceiver
import io.netty.util.{ResourceLeakDetector, ResourceLeakDetectorFactory}

/**
 * Enable reference leak tracking in netty and export a counter at finagle/netty4/reference_leaks.
 *
 * @note By default samples 1% of buffers but this rate can increased via the
 *       io.netty.leakDetectionLevel env variable.
 *
 *       see: https://netty.io/wiki/reference-counted-objects.html#wiki-h3-11
 */
private object trackReferenceLeaks
    extends GlobalFlag[Boolean](
      false,
      "Enable reference leak tracking in Netty and export a counter at finagle/netty4/reference_leaks"
    )

/**
 * Runs prior initialization of any client/server in order to set Netty 4 system properties
 * as early as possible.
 */
private class Netty4Init extends FinagleInit {

  def label: String = "Initializing Netty 4 system properties"

  def apply(): Unit = {

    // We set a sane default and reject client initiated TLS/SSL session
    // renegotiations (for security reasons).
    //
    // NOTE: This property affects both JDK SSL (Java 8+) and Netty 4 OpenSSL
    // implementations.
    if (System.getProperty("jdk.tls.rejectClientInitiatedRenegotiation") == null) {
      System.setProperty("jdk.tls.rejectClientInitiatedRenegotiation", "true")
    }

    // We allocate one arena per a worker thread to reduce contention. By default
    // this will be equal to the number of logical cores * 2.
    //
    // NOTE: Before overriding it, we check whether or not it was set before. This way users
    // will have a chance to tune it.
    //
    // NOTE: Only applicable when pooling is enabled (see `UsePooling`).
    if (System.getProperty("io.netty.allocator.numDirectArenas") == null) {
      System.setProperty("io.netty.allocator.numDirectArenas", numWorkers().toString)
    }

    // Set the number of heap arenas the number of logical cores * 2.
    if (System.getProperty("io.netty.allocator.numHeapArenas") == null) {
      System.setProperty("io.netty.allocator.numHeapArenas", numWorkers().toString)
    }

    // This determines the size of the memory chunks we allocate in arenas. Netty's default
    // is 16mb, we shrink it to 1mb.
    //
    // We make the trade-off between an initial memory footprint and the max buffer size
    // that can still be pooled. Every allocation that exceeds 1mb will fall back
    // to an unpooled allocator.
    //
    // The `io.netty.allocator.maxOrder` (default: 7) determines the number of left binary
    // shifts we need to apply to the `io.netty.allocator.pageSize`
    // (default: 8192): 8192 << 7 = 1mb.
    //
    // NOTE: Before overriding it, we check whether or not it was set before. This way users
    // will have a chance to tune it.
    if (System.getProperty("io.netty.allocator.maxOrder") == null) {
      System.setProperty("io.netty.allocator.maxOrder", "7")
    }

    // Initialize N4 metrics.
    exportNetty4MetricsAndRegistryEntries()

    // Enable tracking of reference leaks.
    if (trackReferenceLeaks()) {
      val referenceLeaks =
        FinagleStatsReceiver.counter("netty4", "reference_leaks")

      if (ResourceLeakDetector.getLevel == ResourceLeakDetector.Level.DISABLED) {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.SIMPLE)
      }

      ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(
        new StatsLeakDetectorFactory({ () =>
          referenceLeaks.incr()
          referenceLeakLintRule.leakDetected()
        })
      )
    }
  }
}
