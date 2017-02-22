package com.twitter.finagle.netty4

import com.twitter.finagle.stats.{InMemoryStatsReceiver, LoadedStatsReceiver}
import com.twitter.finagle.toggle.flag
import io.netty.buffer.ByteBuf
import io.netty.util.ResourceLeakDetectorFactory
import org.scalatest.FunSuite

class StatsLeakDetectorFactoryTest extends FunSuite {
  test("counts netty resource leaks"){
    flag.overrides.let("com.twitter.finagle.netty4.EnableReferenceLeakTracking", 1.0) {
      val sr = new InMemoryStatsReceiver
      LoadedStatsReceiver.self = sr

      trackReferenceLeaks.init
      val detectorFac = ResourceLeakDetectorFactory.instance()

      val detector = detectorFac.newResourceLeakDetector(classOf[ByteBuf])

      // netty's leak detection relies on references getting collected so rather than
      // introduce non-determinism into our test we grab the non-public
      // 'reportTracedLeak' method to fake a leak report.
      val reportLeakMeth = detector.getClass.getMethod("reportTracedLeak", classOf[String], classOf[String])
      reportLeakMeth.invoke(detector, "", "")
      assert(sr.counters(Seq("finagle", "netty4", "reference_leaks")) == 1)
      reportLeakMeth.invoke(detector, "", "")
      assert(sr.counters(Seq("finagle", "netty4", "reference_leaks")) == 2)
      reportLeakMeth.invoke(detector, "", "")
      assert(sr.counters(Seq("finagle", "netty4", "reference_leaks")) == 3)
    }
  }

  test("can load non-bytebuf resource trackers"){
    flag.overrides.let("com.twitter.finagle.netty4.EnableReferenceLeakTracking", 1.0) {
      trackReferenceLeaks.init
      val detectorFac = new StatsLeakDetectorFactory(() => ())
      val detector = detectorFac.newResourceLeakDetector(classOf[String])
    }
  }
}
