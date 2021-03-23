package com.twitter.finagle.offload

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util.FuturePool
import org.mockito.Mockito.when
import org.scalatest.{FunSuite, OneInstancePerTest}
import org.scalatestplus.mockito.MockitoSugar.mock

class OffloadFilterAdmissionControlTest extends FunSuite with OneInstancePerTest {

  private val FastSample: Long = 1l
  private val SlowSample: Long = 5l

  private val mockFuturePool: FuturePool = mock[FuturePool]
  private val stats = new InMemoryStatsReceiver

  private val strictParams = OffloadFilterAdmissionControl.Enabled(
    failurePercentile = 0.0001,
    rejectionIncrement = 1.0,
    queueWaterMark = 1l,
    windowSize = 1
  )

  private var randomValue: Double = 1.0

  private def newAc(
    params: OffloadFilterAdmissionControl.Enabled
  ): OffloadFilterAdmissionControl = {
    // technically we should return a number in the range [0, 1.0)
    // but this should be fine.
    val random = new Rng {
      def nextDouble(): Double = randomValue
      def nextInt(n: Int): Int = ???
      def nextInt(): Int = ???
      def nextLong(n: Long): Long = ???
    }
    new OffloadFilterAdmissionControl(params, mockFuturePool, stats, random)
  }

  test("doesn't reject initially") {
    val ac = newAc(OffloadFilterAdmissionControl.DefaultEnabledParams)
    assert(!ac.shouldReject)
  }

  test("doesn't reject if the queue depth is always 0") {
    val ac = newAc(strictParams)
    when(mockFuturePool.numPendingTasks).thenReturn(0l)
    assert(ac.sample() == SlowSample)
    assert(!ac.shouldReject)
  }

  test("rejects if we exceed the threshold") {
    val ac = newAc(strictParams)
    when(mockFuturePool.numPendingTasks).thenReturn(1l)
    assert(ac.sample() == FastSample)
    assert(ac.shouldReject)
  }

  test("doesnt reject if we exceed the threshold but haven't overflowed the window") {
    val ac = newAc(strictParams.copy(windowSize = 2, failurePercentile = 0.51))
    when(mockFuturePool.numPendingTasks).thenReturn(1l)
    assert(ac.sample() == FastSample)
    assert(!ac.shouldReject)

    assert(ac.sample() == FastSample)
    assert(ac.shouldReject)
  }

  test("recovery phase") {
    val ac = newAc(strictParams.copy(windowSize = 2, failurePercentile = 0.51))
    when(mockFuturePool.numPendingTasks).thenReturn(1l)
    assert(ac.sample() == FastSample)
    assert(!ac.shouldReject)
    assert(ac.sample() == FastSample)
    assert(ac.shouldReject)

    when(mockFuturePool.numPendingTasks).thenReturn(0l)
    assert(ac.sample() == FastSample)
    assert(!ac.shouldReject)

    // Back to healthy sample interval
    assert(ac.sample() == SlowSample)
    assert(!ac.shouldReject)
  }

  test("can recover from an overflow window") {
    val ac = newAc(strictParams.copy(windowSize = 3, failurePercentile = 0.51))
    when(mockFuturePool.numPendingTasks).thenReturn(1l)

    // fill up our window with failure
    assert(ac.sample() == FastSample)
    assert(ac.sample() == FastSample)
    assert(ac.sample() == FastSample)

    assert(ac.shouldReject)

    // Enter recovery phase
    when(mockFuturePool.numPendingTasks).thenReturn(0l)
    assert(ac.sample() == FastSample)

    // probability 1.0
    assert(!ac.shouldReject)
    // We should still not reject because our window says healthy
    randomValue = 0.9
    assert(ac.shouldReject)

    // We should go back to a recovery sample rate and have a zero reject probability
    assert(ac.sample() == FastSample)
    randomValue = 0.0
    assert(!ac.shouldReject)

    // Should be fully in the clear
    assert(ac.sample() == SlowSample)
  }
}
