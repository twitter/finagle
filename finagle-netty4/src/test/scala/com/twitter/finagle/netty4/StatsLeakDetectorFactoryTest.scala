package com.twitter.finagle.netty4

import io.netty.buffer.ByteBuf
import io.netty.util.ResourceLeakDetector
import io.netty.util.ResourceLeakDetectorFactory
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.funsuite.AnyFunSuite

class StatsLeakDetectorFactoryTest extends AnyFunSuite with MockitoSugar {
  test("counts netty resource leaks") {
    var leaks = 0

    val fac = new StatsLeakDetectorFactory(
      mock[ResourceLeakDetectorFactory], // not actually used
      { () => leaks += 1 }
    )
    val detector = fac.newResourceLeakDetector(classOf[ByteBuf])

    // netty's leak detection relies on references getting collected so rather than
    // introduce non-determinism into our test we grab the non-public
    // 'reportTracedLeak' method to fake a leak report.
    val reportLeakMeth =
      detector.getClass.getDeclaredMethod("reportTracedLeak", classOf[String], classOf[String])
    reportLeakMeth.setAccessible(true)
    reportLeakMeth.invoke(detector, "", "")
    assert(leaks == 1)
    reportLeakMeth.invoke(detector, "", "")
    assert(leaks == 2)
    reportLeakMeth.invoke(detector, "", "")
    assert(leaks == 3)
  }

  test("can load non-bytebuf resource trackers") {
    val parent = mock[ResourceLeakDetectorFactory]
    val expected = mock[ResourceLeakDetector[String]]

    when(parent.newResourceLeakDetector(same(classOf[String]), any[Int], any[Long]))
      .thenReturn(expected)

    val detectorFac = new StatsLeakDetectorFactory(parent, () => ())
    val detector = detectorFac.newResourceLeakDetector(classOf[String])
    assert(detector eq expected)
  }
}
