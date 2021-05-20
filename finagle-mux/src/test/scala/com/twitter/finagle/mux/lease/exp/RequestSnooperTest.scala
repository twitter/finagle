package com.twitter.finagle.mux.lease.exp

import com.twitter.util.{MockTimer, Stopwatch, Time}
import com.twitter.conversions.DurationOps._
import com.twitter.conversions.StorageUnitOps._
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class RequestSnooperTest extends AnyFunSuite with MockitoSugar {
  test("RequestSnooper should compute handleBytes reasonably") {
    val ctr = mock[ByteCounter]
    val percentile = 50

    when(ctr.rate()).thenReturn(1)

    val timer = new MockTimer()
    Time.withCurrentTimeFrozen { ctl =>
      when(ctr.lastGc).thenReturn(Time.now - 5.seconds)

      val now = Stopwatch.timeMillis
      val snooper = new RequestSnooper(ctr, percentile, now = now, timer = timer)
      for (_ <- 0 until 50)
        snooper.observe(1.second)
      for (_ <- 0 until 50)
        snooper.observe(2.seconds)
      for (_ <- 0 until 50)
        snooper.observe(3.seconds)
      ctl.advance(12.seconds)
      timer.tick()
      assert(snooper.handleBytes() == 2000.bytes)
    }
  }

  test("RequestSnooper should discard results that overlap with a gc") {
    val ctr = mock[ByteCounter]
    val percentile = 50

    when(ctr.rate()).thenReturn(1)

    val timer = new MockTimer()
    Time.withCurrentTimeFrozen { ctl =>
      when(ctr.lastGc).thenReturn(Time.now - 5.seconds)

      val now = Stopwatch.timeMillis
      val snooper = new RequestSnooper(ctr, percentile, now = now, timer = timer)
      for (_ <- 0 until 50)
        snooper.observe(1.second)
      for (_ <- 0 until 50)
        snooper.observe(2.seconds)
      for (_ <- 0 until 50)
        snooper.observe(3.seconds)
      for (_ <- 0 until 1000)
        snooper.observe(8.seconds)
      ctl.advance(12.seconds)
      timer.tick()
      assert(snooper.handleBytes() == 2000.bytes)
    }
  }
}
