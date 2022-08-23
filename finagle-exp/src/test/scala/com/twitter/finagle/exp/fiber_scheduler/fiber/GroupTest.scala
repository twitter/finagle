package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.conversions.DurationOps.richDurationFromInt
import com.twitter.util.Try
import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec
import com.twitter.finagle.exp.fiber_scheduler.Worker
import com.twitter.finagle.exp.fiber_scheduler.WorkerThread
import com.twitter.util.FuturePool
import java.util.concurrent.atomic.AtomicBoolean

class GroupTest extends FiberSchedulerSpec {

  private trait Context {
    val worker =
      new Worker(() => null, 1.second, WorkerThread.factory.newThread(() => {}))
    var tryScheduleCalls = List.empty[ForkedFiber]
    var scheduleCalls = List.empty[ForkedFiber]
    var tryScheduleResult = true
    def trySchedule(fiber: ForkedFiber): Boolean = {
      if (tryScheduleResult) {
        tryScheduleCalls :+= fiber
      }
      tryScheduleResult
    }
    def schedule(fiber: ForkedFiber): Unit = {
      scheduleCalls :+= fiber
    }
  }
  "default" - {
    "redirects trySchedule to the provided function" in new Context {
      val g = Group.default(trySchedule, schedule)
      val fiber = new ForkedFiber(g)
      assert(g.trySchedule(fiber) == tryScheduleResult)
      assert(tryScheduleCalls == List(fiber))
    }
    "redirects schedule to the provided function" in new Context {
      val g = Group.default(trySchedule, schedule)
      val fiber = new ForkedFiber(g)
      g.schedule(fiber)
      assert(scheduleCalls == List(fiber))
    }
    "done does nothing" in new Context {
      val g = Group.default(trySchedule, schedule)
      val fiber = new ForkedFiber(g)
      g.suspend(fiber)
    }
  }
  "withMaxSyncConcurrency" - {
    "trySchedule" - {
      "respects concurrency" in new Context {
        val concurrency = 10
        val maxWaiters = 0
        val g = Group.withMaxSyncConcurrency(concurrency, maxWaiters, trySchedule, schedule)
        val fibers = List.fill(concurrency)(new ForkedFiber(g))
        fibers.foreach(f => assert(g.trySchedule(f)))
        assert(!g.trySchedule(new ForkedFiber(g)))
        assert(tryScheduleCalls == fibers)
      }
      "bounded waiters" in new Context {
        val concurrency = 10
        val maxWaiters = 5
        val g = Group.withMaxSyncConcurrency(concurrency, maxWaiters, trySchedule, schedule)
        val fibers = List.fill(concurrency + maxWaiters)(new ForkedFiber(g))
        fibers.foreach(f => assert(g.trySchedule(f)))
        assert(!g.trySchedule(new ForkedFiber(g)))
        assert(tryScheduleCalls == fibers.take(concurrency))
      }
      "unbounded waiters" in new Context {
        val concurrency = 10
        val maxWaiters = Int.MaxValue
        val g = Group.withMaxSyncConcurrency(concurrency, maxWaiters, trySchedule, schedule)
        val fibers1 = List.fill(concurrency)(new ForkedFiber(g))
        val fibers2 = List.fill(concurrency)(new ForkedFiber(g))
        fibers1.foreach(f => assert(g.trySchedule(f)))
        fibers2.foreach(f => assert(g.trySchedule(f)))
        assert(tryScheduleCalls == fibers1)
        fibers1.foreach(g.suspend)
        assert(tryScheduleCalls == fibers1)
        assert(scheduleCalls == fibers2)
      }
      "handles rejections from the underlying trySchedule function" in new Context {
        val concurrency = 10
        val maxWaiters = 0
        val g = Group.withMaxSyncConcurrency(concurrency, maxWaiters, trySchedule, schedule)
        val fibers1 = List.fill(concurrency / 2)(new ForkedFiber(g))
        val fibers2 = List.fill(concurrency / 2)(new ForkedFiber(g))

        fibers1.foreach(f => assert(g.trySchedule(f)))
        assert(tryScheduleCalls == fibers1)

        tryScheduleResult = false
        fibers2.foreach(f => assert(!g.trySchedule(f)))
        assert(tryScheduleCalls == fibers1)

        tryScheduleResult = true
        fibers2.foreach(f => assert(g.trySchedule(f)))
        assert(tryScheduleCalls == fibers1 ::: fibers2)
      }
      "properly handles a waiter added while a trySchedule is running" in new Context {
        val concurrency = 1
        val maxWaiters = 1
        val g = Group.withMaxSyncConcurrency(concurrency, maxWaiters, trySchedule, schedule)
        val cdl = CountDownLatch(1)
        val fiber = new ForkedFiber(g)
        val waiting = new AtomicBoolean(false)
        // submit a fiber in another thread that blocks on the CountDownLatch
        // while trying to schedule
        override def trySchedule(fiber: ForkedFiber): Boolean = {
          waiting.set(true)
          cdl.await()
          false
        }
        val fut =
          FuturePool.unboundedPool {
            g.trySchedule(new ForkedFiber(g))
          }
        eventually(waiting.get())
        // add one waiter
        g.schedule(fiber)
        // release the initial trySchedule call
        cdl.countDown()
        // the trySchedule in the other thread returns false
        assert(!await(fut))
        // the waiter is released and scheduled
        assert(scheduleCalls == List(fiber))
      }
    }

    "schedule" - {
      "respects concurrency" in new Context {
        val concurrency = 10
        val maxWaiters = 0
        val g = Group.withMaxSyncConcurrency(concurrency, maxWaiters, trySchedule, schedule)
        val fibers = List.fill(concurrency)(new ForkedFiber(g))
        fibers.foreach(f => g.schedule(f))
        assert(Try(g.schedule(new ForkedFiber(g))).isThrow)
        assert(scheduleCalls == fibers)
      }
      "bounded waiters" in new Context {
        val concurrency = 10
        val maxWaiters = 5
        val g = Group.withMaxSyncConcurrency(concurrency, maxWaiters, trySchedule, schedule)
        val fibers = List.fill(concurrency + maxWaiters)(new ForkedFiber(g))
        fibers.foreach(f => g.schedule(f))
        assert(Try(g.schedule(new ForkedFiber(g))).isThrow)
        assert(scheduleCalls == fibers.take(concurrency))
      }
      "unbounded waiters" in new Context {
        val concurrency = 10
        val maxWaiters = Int.MaxValue
        val g = Group.withMaxSyncConcurrency(concurrency, maxWaiters, trySchedule, schedule)
        val fibers1 = List.fill(concurrency)(new ForkedFiber(g))
        val fibers2 = List.fill(concurrency)(new ForkedFiber(g))
        fibers1.foreach(f => g.schedule(f))
        fibers2.foreach(f => g.schedule(f))
        assert(scheduleCalls == fibers1)
        fibers1.foreach(g.suspend)
        assert(scheduleCalls == (fibers1 ::: fibers2))
      }
    }
  }
}
