package com.twitter.finagle.offload

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Future, FuturePool, Promise, Try}
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class OffloadFilterAdmissionControlTest extends AnyFunSuite with OneInstancePerTest {

  private val FastSample: Long = 1l
  private val SlowSample: Long = 5l

  private class MockFuturePool extends FuturePool {

    private case class Task[T](f: () => T, p: Promise[T]) {
      def run(): Unit = p.updateIfEmpty(Try(f()))
    }

    private var thunks: Vector[Task[_]] = Vector.empty
    def apply[T](f: => T): Future[T] = {
      val p = Promise[T]()
      thunks :+= Task(() => f, p)
      p
    }

    override def numPendingTasks: Long = thunks.size

    def runOne(): Unit = {
      if (thunks.isEmpty) {
        throw new NoSuchElementException()
      }
      val head = thunks.head
      thunks = thunks.tail
      head.run()
    }

    def runAll(): Unit = {
      while (thunks.nonEmpty) {
        runOne()
      }
    }
  }

  private val mockFuturePool: MockFuturePool = new MockFuturePool
  private val stats = new InMemoryStatsReceiver

  // Admission control will kick in once the pending decrements grows past 1 task.
  private val strictParams =
    OffloadACConfig.Enabled(maxQueueDelay = 1.milliseconds)

  private def newAc(
    params: OffloadACConfig.Enabled
  ): OffloadFilterAdmissionControl = {
    new OffloadFilterAdmissionControl(params, mockFuturePool, stats)
  }

  test("doesn't reject initially") {
    val ac = newAc(OffloadACConfig.DefaultEnabledParams)
    assert(!ac.shouldReject)
  }

  test("doesn't reject if the queue depth is always 0") {
    val ac = newAc(strictParams)
    (0 until 10).foreach { _ =>
      assert(ac.sample() == SlowSample)
      assert(mockFuturePool.numPendingTasks == 0l)
      assert(!ac.shouldReject)
    }
  }

  test("going into and out of rejection mode") {
    val ac = newAc(strictParams)

    // Off some work to the queue to start things off.
    val p = mockFuturePool { true }
    assert(!ac.shouldReject)
    assert(mockFuturePool.numPendingTasks == 1l)

    // A single probe isn't enough to reject
    assert(ac.sample() == FastSample)
    assert(!ac.shouldReject)
    assert(mockFuturePool.numPendingTasks == 2l)

    // Now we're over the watermark
    assert(ac.sample() == FastSample)
    assert(ac.shouldReject)
    assert(mockFuturePool.numPendingTasks == 3l)

    // Clear the simple task at the time but that doesn't stop the AC since we still have probes
    assert(!p.isDefined)
    mockFuturePool.runOne()
    assert(p.isDefined)
    assert(ac.shouldReject)
    assert(mockFuturePool.numPendingTasks == 2l)

    // Run one and then there is only one probe outstanding and thus no rejection
    mockFuturePool.runOne()
    assert(!ac.shouldReject)
    assert(mockFuturePool.numPendingTasks == 1l)

    // back over the watermark with two probes
    assert(ac.sample() == FastSample)
    assert(ac.shouldReject)
    assert(mockFuturePool.numPendingTasks == 2l)

    // Clear the queue
    mockFuturePool.runAll()
    assert(ac.sample() == SlowSample)
    assert(!ac.shouldReject)
    assert(mockFuturePool.numPendingTasks == 0l)
  }
}
