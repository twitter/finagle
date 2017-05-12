package com.twitter.finagle.netty4

import com.twitter.finagle.toggle.flag
import io.netty.buffer.ByteBuf
import org.scalatest.FunSuite

class StatsLeakDetectorFactoryTest extends FunSuite {
  test("counts netty resource leaks"){
    flag.overrides.let("com.twitter.finagle.netty4.EnableReferenceLeakTracking", 1.0) {

      var leaks = 0
      val fac = new StatsLeakDetectorFactory({ () => leaks += 1 })
      val detector = fac.newResourceLeakDetector(classOf[ByteBuf])

      // netty's leak detection relies on references getting collected so rather than
      // introduce non-determinism into our test we grab the non-public
      // 'reportTracedLeak' method to fake a leak report.
      val reportLeakMeth = detector.getClass.getDeclaredMethod("reportTracedLeak", classOf[String], classOf[String])
      reportLeakMeth.setAccessible(true)
      reportLeakMeth.invoke(detector, "", "")
      assert(leaks == 1)
      reportLeakMeth.invoke(detector, "", "")
      assert(leaks == 2)
      reportLeakMeth.invoke(detector, "", "")
      assert(leaks == 3)
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
