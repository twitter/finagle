package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.util.Duration
import com.twitter.util.Promise
import java.util.concurrent.Executors
import java.util.concurrent.locks.LockSupport
import com.twitter.finagle.exp.fiber_scheduler.fiber.ForkedFiber
import com.twitter.finagle.exp.fiber_scheduler.fiber.Group

class WorkerTest extends FiberSchedulerSpec {

  "enqueue" - {
    "adds fiber to mailbox" in new Context {
      var executed = List.empty[Int]
      val w = newWorker()
      val f = newFiber {
        executed :+= 1
        stopWorker(w)
      }
      assert(w.enqueue(f, false))
      w.run()
      w.cleanup()
      assert(executed == List(1))
    }
    "updates load" in new Context {
      val w = newWorker()
      assert(w.enqueue(newFiber(), false))
      assert(w.load() == 1)
      assert(w.enqueue(newFiber(), true))
      assert(w.load() == 2)
    }
  }

  "steal" - {
    "returns null if load <= 1" in new Context {
      val w = newWorker()
      assert(w.steal() == null)
      w.enqueue(newFiber(), true)
      assert(w.steal() == null)
    }
    "steals the last fiber if load > 1" in new Context {
      val w = newWorker()
      w.enqueue(newFiber(), true)
      val f = newFiber()
      w.enqueue(f, true)
      assert(w.steal() == f)
    }
  }

  "run" - {
    "stops when the worker is stopped" in new Context {
      var executed = List.empty[Int]
      val w = newWorker()
      def createFiber(i: Int) = newFiber {
        executed :+= i
        stopWorker(w)
      }
      val f1 = createFiber(1)
      val f2 = createFiber(2)
      assert(w.enqueue(f1, false))
      assert(w.enqueue(f2, true))
      w.run()
      w.cleanup()
      if (Config.Mailbox.workerMailboxFifo) {
        assert(executed == List(1))
      } else {
        assert(executed == List(2))
      }
    }
    "tries to steal a fiber if has no tasks" in new Context {
      var triedToSteal = 0
      val w = newWorker()
      override def stealFiber() = {
        triedToSteal += 1
        stopWorker(w)
        null
      }
      w.run()
      w.cleanup()
      assert(triedToSteal == 1)
    }
    "runs the stolen fiber" in new Context {
      var executions = 0
      val w = newWorker()
      val fiber = newFiber {
        executions += 1
        stopWorker(w)
      }
      override def stealFiber() = fiber

      w.run()
      w.cleanup()
      assert(executions == 1)
    }
    "returns backlog when stopped" in new Context {
      val w = newWorker()
      val f1 = newFiber()
      val f2 = newFiber()
      assert(w.enqueue(f1, force = false))
      assert(w.enqueue(f2, force = true))
      stopWorker(w)
      assert(w.run() == List(f1, f2))
      w.cleanup()
    }
    "doesn't allow enqueuing if stopped" in new Context {
      val w = newWorker()
      stopWorker(w)
      w.run()
      w.cleanup()
      assert(!w.enqueue(newFiber(), force = false))
    }
    "parks if it has nothing to execute and doesn't need to stop" in new Context {
      val exec = Executors.newCachedThreadPool()
      @volatile var workerThread: Thread = null
      @volatile var worker: Worker = null
      try {
        exec.execute(() => {
          worker = newWorker()
          workerThread = Thread.currentThread()
          worker.run()
          worker.cleanup()
        })
        def blocker = LockSupport.getBlocker(workerThread)
        eventually(workerThread != null && blocker == worker)
        stopWorker(worker)
      } finally {
        exec.shutdown()
      }
    }
  }

  private trait Context {
    def stealFiber(): ForkedFiber = null
    def stopWorker(w: Worker) = assert(w.stop(Promise()))
    def newFiber(task: => Unit) = {
      val f = new ForkedFiber(Group.default(_ => true, _ => {}))
      f.add(() => task, asOwner = true)
      f
    }
    def newWorker() = new Worker(stealFiber, Duration.Top, WorkerThread.factory.newThread(() => {}))
    def withBlockedWorker(f: (Worker, ForkedFiber, () => Unit) => Unit) = {
      val exec = Executors.newCachedThreadPool()
      try {
        val fiber = newFiber()
        val cdl = CountDownLatch(1)
        @volatile var waiting = false
        fiber.add(
          () => {
            waiting = true
            cdl.await()
          },
          asOwner = true)

        @volatile var workerThread: Thread = null
        val w = exec
          .submit(() => {
            workerThread = Thread.currentThread()
            newWorker
          }).get()

        assert(w.enqueue(fiber, false))

        exec.submit(() => w.run())
        def blocker = LockSupport.getBlocker(workerThread)
        while (!waiting || blocker == null) {
          Thread.sleep(10)
        }
        f(
          w,
          fiber,
          () => {
            cdl.countDown()
            while (blocker != w) {
              Thread.sleep(10)
            }
          })
        cdl.countDown()
        stopWorker(w)
      } finally {
        exec.shutdown()
      }
    }
  }
}
