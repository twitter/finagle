package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.finagle.exp.fiber_scheduler.Config
import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec
import com.twitter.finagle.exp.fiber_scheduler.Worker
import com.twitter.finagle.exp.fiber_scheduler.WorkerThread

class ForkedFiberTest extends FiberSchedulerSpec {

  "hasExecutor" in new Context {
    assert(fiber.hasExecutor)
  }

  "wake up" - {
    "does nothing if bias is not set" in new Context {
      submitTask()
      assert(scheduled.isEmpty)
    }
    "schedules if bias is set" in new Context {
      fiber.run(worker)
      submitTask()
      assert(scheduled == List(fiber))
    }
    "doesn't schedule twice" in new Context {
      fiber.run(worker)
      submitTask()
      submitTask()
      assert(scheduled == List(fiber))
    }
  }

  "run" - {
    "executes the fiber's tasks" in new Context {
      var executed = List.empty[Int]
      fiber.add(() => executed :+= 1, asOwner = false)
      fiber.add(() => executed :+= 2, asOwner = true)
      fiber.run(worker)
      if (Config.Mailbox.fiberMailboxFifo) {
        assert(executed == List(1, 2))
      } else {
        assert(executed == List(2, 1))
      }
    }
    "doesn't stop after time slice if worker's mailbox is empty" in new Context {
      var executed = List.empty[Int]
      fiber.add(
        () => {
          executed :+= 1
          Thread.sleep(Config.Scheduling.fiberTimeSlice.inMillis + 100)
        },
        asOwner = true)
      fiber.add(() => executed :+= 2, asOwner = false)
      fiber.run(worker)
      assert(executed == List(1, 2))
    }
    "stops after time slice if worker's mailbox is not empty" in new Context {
      worker.enqueue(new ForkedFiber(Group.default(trySchedule, schedule)), force = true)
      var executed = List.empty[Int]
      fiber.add(
        () => {
          executed :+= 1
          Thread.sleep(Config.Scheduling.fiberTimeSlice.inMillis + 100)
        },
        asOwner = true)
      fiber.add(() => executed :+= 2, asOwner = false)
      fiber.run(worker)
      assert(executed == List(1))
      fiber.run(worker)
      assert(executed == List(1, 2))
    }
  }

  private trait Context {
    val worker = new Worker(() => null, Duration.Top, WorkerThread.factory.newThread(() => {}))
    var scheduled = List.empty[ForkedFiber]
    def trySchedule(f: ForkedFiber): Boolean = {
      scheduled +:= f
      true
    }
    def schedule(f: ForkedFiber): Boolean = {
      scheduled +:= f
      true
    }
    val fiber = new ForkedFiber(Group.default(trySchedule, schedule))
    var taskExecutions = 0
    def submitTask() = fiber.submit(Future.value(taskExecutions += 1), false, _ => {})
  }
}
