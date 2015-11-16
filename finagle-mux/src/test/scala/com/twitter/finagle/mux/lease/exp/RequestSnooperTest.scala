package com.twitter.finagle.mux.lease.exp

import com.twitter.util.{Time, MockTimer}
import com.twitter.conversions.time.intToTimeableNumber
import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import org.junit.runner.RunWith
import org.mockito.Mockito.{when, verify, times}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class RequestSnooperTest extends FunSuite with MockitoSugar {
  test("RequestSnooper should compute handleBytes reasonably") {
    val ctr = mock[ByteCounter]
    val quantile = 0.50

    when(ctr.rate()).thenReturn(1)

    Time.withCurrentTimeFrozen { ctl =>
      when(ctr.lastGc).thenReturn(Time.now - 5.seconds)

      val tmr = new MockTimer()
      val snooper = new RequestSnooper(ctr, quantile, timer = tmr)
      for (_ <- 0 until 50)
        snooper.observe(1.second)
      for (_ <- 0 until 50)
        snooper.observe(2.seconds)
      for (_ <- 0 until 50)
        snooper.observe(3.seconds)
      ctl.advance(12.seconds)
      tmr.tick()
      assert(snooper.handleBytes() == 2000.bytes)
    }
  }

  test("RequestSnooper should discard results that overlap with a gc") {
    val ctr = mock[ByteCounter]
    val quantile = 0.50

    when(ctr.rate()).thenReturn(1)

    Time.withCurrentTimeFrozen { ctl =>
      when(ctr.lastGc).thenReturn(Time.now - 5.seconds)

      val tmr = new MockTimer()
      val snooper = new RequestSnooper(ctr, quantile, timer = tmr)
      for (_ <- 0 until 50)
        snooper.observe(1.second)
      for (_ <- 0 until 50)
        snooper.observe(2.seconds)
      for (_ <- 0 until 50)
        snooper.observe(3.seconds)
      for (_ <- 0 until 1000)
        snooper.observe(8.seconds)
      ctl.advance(12.seconds)
      tmr.tick()
      assert(snooper.handleBytes() == 2000.bytes)
    }
  }
}
