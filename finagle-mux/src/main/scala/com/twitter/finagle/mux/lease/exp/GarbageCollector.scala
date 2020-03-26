package com.twitter.finagle.mux.lease.exp

import java.util.logging.Level

private[lease] object GarbageCollector {
  private val log = java.util.logging.Logger.getLogger("GarbageCollector")

  val forceNewGc: () => Unit =
    try {
      // This is a method present in Twitter's JVMs to force
      // a minor collection.
      val meth = Class.forName("com.twitter.hotspot.System").getMethod("minorGc")
      log.log(Level.INFO, "Found com.twitter.hotspot.System.minorGc")
      () => meth.invoke(null)
    } catch {
      case exc: ClassNotFoundException =>
        log.log(
          Level.INFO,
          "Failed to resolve com.twitter.hotspot.System; falling " +
            "back to full GC",
          exc
        )
        () => System.gc()
    }
}
