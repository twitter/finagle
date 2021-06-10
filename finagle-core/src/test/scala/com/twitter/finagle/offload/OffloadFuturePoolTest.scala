package com.twitter.finagle.offload

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Future, FuturePool, MockTimer, Time}
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ArrayBuffer

class OffloadFuturePoolTest extends AnyFunSuite {

  class MockFuturePool extends FuturePool {
    private val queue = ArrayBuffer.empty[() => Any]
    def apply[T](f: => T): Future[T] = {
      queue += { () => f }
      Future.never
    }

    def runAll(): Unit = {
      while (queue.nonEmpty) {
        queue.remove(0).apply()
      }
    }

    def isEmpty: Boolean = queue.isEmpty

    override def numPendingTasks: Long = queue.size
  }

  test("sample delay should sample the stats") {
    val stats = new InMemoryStatsReceiver
    val pool = new MockFuturePool
    val timer = new MockTimer
    val sampleDelay = new SampleQueueStats(pool, stats, timer)
    Time.withCurrentTimeFrozen { ctrl =>
      sampleDelay()

      ctrl.advance(50.milliseconds)
      pool.runAll()
      assert(stats.stats(Seq("delay_ms")) == Seq(50))
      assert(stats.stats(Seq("pending_tasks")) == Seq(0))
      assert(timer.tasks.nonEmpty)
      assert(pool.isEmpty)

      ctrl.advance(50.milliseconds)
      timer.tick()
      assert(timer.tasks.isEmpty)
      assert(!pool.isEmpty)

      ctrl.advance(200.milliseconds)
      pool(()) // one pending task
      pool.runAll()

      assert(stats.stats(Seq("delay_ms")) == Seq(50, 200))
      assert(stats.stats(Seq("pending_tasks")) == Seq(0, 1))
    }
  }
}
