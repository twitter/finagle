package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.concurrent.Scheduler
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Local
import com.twitter.util.Promise
import java.util.concurrent.Executors

/**
 * Test suite to be used for code walkthrough
 */
class WalkthroughTest extends FiberSchedulerSpec {

  "abstractions the fiber scheduler is built on top of" - {

    "Scheduler" - {
      "is a global value" in {
        val current = Scheduler()
        Scheduler.setUnsafe(current)
      }
      "executes future continuations" in {
        val p = Promise[Int]()
        val fut =
          p.map { i =>
              i + 1
            }.map { i =>
              i + 2
            }
        p.setValue(1)
        assert(await(fut) == 4)
      }
      "avoids stack overflows" in {
        def loop(f: Future[Int]): Future[Unit] =
          f.flatMap {
            case 0 => Future.Done
            case i => loop(Future.value(i - 1))
          }
        await(loop(Future.value(10000)))
      }
    }

    "Local" - {
      "like thread locals but for async" in {
        val l = new Local[Int]
        val fut =
          l.let(10) {
            Future.value(10).map { i =>
              val v = l()
              i + v.get
            }
          }
        assert(await(fut) == 20)
      }
      "are present even after async boundaries" in {
        val l = new Local[Int]
        val p = Promise[Int]()
        val fut =
          l.let(10) {
            p.map { i =>
              val v = l()
              i + v.get
            }
          }
        p.setValue(1)
        assert(await(fut) == 11)
      }
    }
  }

  "types of fibers" - {
    "thread local fiber" in withFiberScheduler() { s =>
      val p = Promise[Int]()
      val fut =
        p.map { i =>
            i + 1
          }.map { i =>
            i + 2
          }
      p.setValue(1)
      assert(await(fut) == 4)
    }
    "forked fiber" in withFiberScheduler() { s =>
      val p = Promise[Int]()
      val fut =
        s.fork {
          p.map { i =>
              i + 1
            }.map { i =>
              i + 2
            }
        }
      p.setValue(1)
      assert(await(fut) == 4)
    }
    "executor fiber" in withFiberScheduler() { s =>
      val p = Promise[Int]()
      val exec = Executors.newCachedThreadPool()
      val fut =
        s.fork(exec) {
          p.map { i =>
              i + 1
            }.map { i =>
              i + 2
            }
        }
      p.setValue(1)
      assert(await(fut) == 4)
      exec.shutdown()
    }
  }

  def withFiberScheduler(
    threads: Int = 4,
    adaptive: Boolean = true,
    maxQueuingDelay: Duration = Duration.Top
  )(
    f: FiberScheduler => Unit
  ) = {
    val s = new FiberScheduler(threads, adaptive, threads / 2, threads * 2, maxQueuingDelay)
    val prev = Scheduler()
    Scheduler.setUnsafe(s)
    try f(s)
    finally {
      s.shutdown()
      Scheduler.setUnsafe(prev)
    }
  }
}
