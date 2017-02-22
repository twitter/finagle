package com.twitter.finagle.netty4

import com.twitter.finagle.toggle.flag
import io.netty.buffer.ByteBuf
import io.netty.util.ResourceLeakDetectorFactory
import org.scalatest.FunSuite

class ReferenceLeakRuleTest extends FunSuite {
  test("lint rule reports reference leaks"){
    flag.overrides.let("com.twitter.finagle.netty4.EnableReferenceLeakTracking", 1.0) {
      trackReferenceLeaks.init
      val fac = ResourceLeakDetectorFactory.instance()

      val rule = trackReferenceLeaks.leakDetectedRule

      val detector = fac.newResourceLeakDetector(classOf[ByteBuf])

      assert(rule() == Nil)
      // netty's leak detection relies on references getting collected so rather than
      // introduce non-determinism into our test we grab the non-public
      // 'reportTracedLeak' method to fake a leak report.
      val reportLeakMeth = detector.getClass.getMethod("reportTracedLeak", classOf[String], classOf[String])
      reportLeakMeth.invoke(detector, "", "")

      assert(rule().length == 1)
    }
  }
}
