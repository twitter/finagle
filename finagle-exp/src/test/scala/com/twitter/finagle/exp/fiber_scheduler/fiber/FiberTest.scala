package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Local
import com.twitter.util.Promise
import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec
import com.twitter.finagle.exp.fiber_scheduler.Worker
import com.twitter.finagle.exp.fiber_scheduler.WorkerThread

class FiberTest extends FiberSchedulerSpec {

  val onComplete = (completions: Int) => {}

  "add" -
    withFiber { (fiber, run, asOwner) =>
      var count = 0
      fiber.add(() => count += 1, asOwner)
      run()
      assert(count == 1)
    }

  "submit" - {
    "evalutates the future as a task" -
      withFiber { (fiber, run, asOwner) =>
        val res =
          fiber.submit(
            {
              Future.value(1)
            },
            asOwner,
            onComplete)
        run()
        assert(await(res) == 1)
      }
    "propagates locals" -
      withFiber { (fiber, run, asOwner) =>
        val l = new Local[Int]()
        l.set(Some(1))
        val res =
          fiber.submit(
            {
              val v = l()
              l.set(Some(2))
              Future.value(v)
            },
            asOwner,
            onComplete)
        run()
        assert(l() == Some(1))
        assert(await(res) == Some(1))
      }
    "propagates interrupts" -
      withFiber { (fiber, run, asOwner) =>
        val p = Promise[Int]()
        val ex = new Exception
        var intr: Throwable = null
        p.setInterruptHandler {
          case ex =>
            intr = ex
        }
        val res = fiber.submit(p.map(_ + 1), asOwner, onComplete)
        run()
        res.raise(ex)
        run()
        p.ensure(println)
        assert(intr == ex)
      }
  }

  private def withFiber(f: (Fiber, () => Unit, Boolean) => Unit) = {
    def test(asOwner: Boolean) = {
      "forked" in {
        val fiber = new ForkedFiber(Group.default(_ => true, _ => {}))
        f(
          fiber,
          () => {
            fiber.add(() => {}, false)
            fiber.run(
              new Worker(() => null, Duration.Top, WorkerThread.factory.newThread(() => {})))
          },
          asOwner)
      }
      "local" in {
        val fiber = new ThreadLocalFiber
        f(fiber, () => {}, asOwner)
      }
      "executor" in {
        var tasks = List.empty[Runnable]
        val fiber = new ExecutorFiber(tasks :+= _)
        f(
          fiber,
          () => {
            fiber.add(() => {}, false)
            tasks.foreach(_.run())
          },
          asOwner)
      }
    }
    "asOwner" - test(true)
    "asOther" - test(false)
  }

}
