package com.twitter.finagle.netty4

import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import io.netty.buffer.ByteBuf
import com.twitter.finagle.toggle.flag
import org.scalatest.FunSuite

class StatsLeakDetectorFactoryTest extends FunSuite {
  test("counts netty resource leaks"){
    System.setProperty("io.netty.leakDetectionLevel", "paranoid")
    val sr = new InMemoryStatsReceiver

    val detectorFac = new StatsLeakDetectorFactory(sr)

    val detector = detectorFac.newResourceLeakDetector(classOf[ByteBuf])

    // netty's leak detection relies on references getting collected so rather than
    // introduce non-determinism into our test we grab the non-public
    // 'reportTracedLeak' method to fake a leak report.
    val reportLeakMeth = detector.getClass.getMethod("reportTracedLeak", classOf[String], classOf[String])
    reportLeakMeth.invoke(detector, "", "")
    assert(sr.counters(Seq("netty4", "reference_leaks")) == 1)
    reportLeakMeth.invoke(detector, "", "")
    assert(sr.counters(Seq("netty4", "reference_leaks")) == 2)
    reportLeakMeth.invoke(detector, "", "")
    assert(sr.counters(Seq("netty4", "reference_leaks")) == 3)

    System.setProperty("io.netty.leakDetectionLevel", "disabled")
  }

  test("can load non-bytebuf resource trackers"){
    flag.overrides.let("com.twitter.finagle.netty4.EnableReferenceLeakTracking", 1.0) {
      trackReferenceLeaks.init
      val detectorFac = new StatsLeakDetectorFactory(NullStatsReceiver)
      val detector = detectorFac.newResourceLeakDetector(classOf[String])
    }
  }
}
