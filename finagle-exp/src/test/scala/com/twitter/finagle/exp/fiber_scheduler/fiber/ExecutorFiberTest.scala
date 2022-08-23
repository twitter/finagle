package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec
import java.util.concurrent.Executor

class ExecutorFiberTest extends FiberSchedulerSpec {

  "hasExecutor" - new Context {
    assert(fiber.hasExecutor == true)
  }

  "wakeUp" - {
    "schedules for execution" in new Context {
      submitTask()
      assert(executorTasks.size == 1)
    }
    "doesn't schedule again if already scheduled" in new Context {
      submitTask()
      assert(executorTasks.size == 1)
    }
    "schedules again after execution" in new Context {
      submitTask()
      assert(executorTasks.size == 1)
      executorTasks.head.run()
      assert(executorTasks.size == 1)
    }
    "schedules runnable that executes the fiber's tasks" in new Context {
      submitTask()
      assert(executorTasks.size == 1)
      executorTasks.head.run()
      assert(taskExecutions == 1)
    }
  }

  trait Context {
    var executorTasks = List.empty[Runnable]
    val executor: Executor = (r: Runnable) => executorTasks :+= r
    val fiber = new ExecutorFiber(executor)
    var taskExecutions = 0
    def submitTask() = fiber.add(() => taskExecutions += 1, false)
  }
}
