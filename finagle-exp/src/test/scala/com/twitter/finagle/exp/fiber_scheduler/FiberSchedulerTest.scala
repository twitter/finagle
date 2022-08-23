package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.conversions.DurationOps._
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger
import com.twitter.concurrent.Scheduler
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Time
import com.twitter.util.Try

class FiberSchedulerTest extends FiberSchedulerSpec {

  "uses the current thread if not forked" in
    withScheduler() { _ =>
      val t = Thread.currentThread()
      var ensureCalled = false
      val fut =
        Future
          .value(1)
          .flatMap { i =>
            assert(t == Thread.currentThread())
            Future.value(i + 1)
          }.map { i =>
            assert(t == Thread.currentThread())
            i + 1
          }.ensure {
            assert(t == Thread.currentThread())
            ensureCalled = true
          }
      assert(await(fut) == 3)
      assert(ensureCalled)
    }

  "tryFork" - {
    // currently flaky, ignoring for now
    "doesn't reject work if the scheduler isn't overloaded" ignore
      withScheduler(threads = 11, maxQueuingDelay = 5.millis) { s =>
        val cdls = List.fill(10)(CountDownLatch(1))
        cdls.foreach(cdl =>
          s.fork {
            cdl.await()
            Future.Done
          })
        Time.sleep(50.millis)
        assert(await(s.tryFork(Future.Done)).nonEmpty)
        cdls.foreach(_.countDown())
      }
    // currently flaky, ignoring for now
    "rejects work if the scheduler is overloaded" ignore
      withScheduler(threads = 10, maxQueuingDelay = 5.millis) { s =>
        val cdls = List.fill(10)(CountDownLatch(1))
        cdls.foreach(cdl =>
          s.fork {
            cdl.await()
            Future.Done
          })
        Time.sleep(50.millis)
        assert(await(s.tryFork(Future.Done)).isEmpty)
        cdls.foreach(_.countDown())
      }
  }

  "fork" - {
    // currently flaky, ignoring for now
    "doesn't reject work if the scheduler is overloaded" ignore
      withScheduler(threads = 10, maxQueuingDelay = 5.millis) { s =>
        val cdls = List.fill(10)(CountDownLatch(1))
        cdls.foreach(cdl =>
          s.fork {
            cdl.await()
            Future.Done
          })
        Time.sleep(50.millis)
        await(s.fork(Future.Done))
        cdls.foreach(_.countDown())
      }
    "executes the forked function in another thread" in
      withScheduler() { s =>
        @volatile var calledFrom: Thread = null
        val fut =
          s.fork {
            calledFrom = Thread.currentThread()
            Future.value(1)
          }
        assert(await(fut) == 1)
        assert(calledFrom != null)
        assert(calledFrom != Thread.currentThread())
      }
    // currently flaky, ignoring for now
    "respects the number of threads if not adaptive" ignore
      withScheduler(threads = 2, adaptive = false) { s =>
        val running = new AtomicInteger
        val cdl = CountDownLatch(1)
        def task(i: Int) =
          s.fork {
            running.incrementAndGet()
            cdl.await()
            Future.value(i)
          }

        val f1 = task(1)
        val f2 = task(2)
        val f3 = task(3)

        eventually(running.get() == 2)
        Thread.sleep(100)
        assert(running.get() == 2)

        cdl.countDown()
        assert(await(f1) == 1)
        assert(await(f2) == 2)
        assert(await(f3) == 3)
      }

    // currently flaky, ignoring for now
    "grows if adaptive" ignore
      withScheduler(threads = 3, adaptive = true) { s =>
        val running = new AtomicInteger
        def task(i: Int) = {
          val cdl = CountDownLatch(1)
          val fut =
            s.fork {
              running.incrementAndGet()
              cdl.await()
              running.decrementAndGet()
              Future.value(i)
            }
          (fut, cdl)
        }
        val (f1, cdl1) = task(1)
        val (f2, cdl2) = task(2)
        val (f3, cdl3) = task(3)
        val (f4, cdl4) = task(4)
        eventually(running.get() == 4)
        cdl1.countDown()
        cdl2.countDown()
        cdl3.countDown()
        cdl4.countDown()
        assert(await(f1) == 1)
        assert(await(f2) == 2)
        assert(await(f3) == 3)
        assert(await(f4) == 4)
      }

    "blocking" - {
      "fiber can create async tasks that are fulfilled by other threads while it's blocked" in
        withScheduler(threads = 5, maxQueuingDelay = 5.millis) { s =>
          val r =
            s.fork {
              val t = Thread.currentThread()
              val fut = Future.value(1).delayed(1.second)(DefaultTimer).map { i =>
                assert(t != Thread.currentThread())
                i + 1
              }
              awaitReady(fut)
            }
          assert(await(r) == 2)
        }
      "fiber doesn't migrate to another worker after a blocking operation" in
        withScheduler(threads = 5, maxQueuingDelay = 5.millis) { s =>
          val r =
            s.fork {
              val thread = Thread.currentThread()
              val fut = Future.value(1).delayed(1.second)(DefaultTimer).map(_ + 1)
              awaitReady(fut).flatMap { i =>
                assert(thread == Thread.currentThread())
                Future.value(i)
              }
            }
          await(r)
        }
      "continuations are executed by the fiber once the blocking operation is finished" in
        withScheduler(threads = 1, adaptive = false) { s =>
          val r =
            s.fork {
              val thread = Thread.currentThread()
              val p = Promise[Int]()
              val fut = Future.value(1).delayed(1.second)(DefaultTimer).map(_ + 1)
              val fut2 = fut.flatMap { i =>
                p.flatMap { j =>
                  assert(thread == Thread.currentThread())
                  Future.value(i + j)
                }
              }
              awaitReady(fut)
              p.setValue(2)
              fut2.flatMap { i =>
                assert(thread == Thread.currentThread())
                Future.value(i)
              }
            }
          await(r)
        }
    }
  }
  "fork with executor" - {
    "uses the provided executor" in
      withScheduler() { s =>
        var runs = 0
        val t = Thread.currentThread()
        val exec: Executor =
          (r: Runnable) => {
            runs += 1
            r.run()
          }
        val fut =
          s.fork(exec) {
            assert(t == Thread.currentThread())
            Future.value(1)
          }
        assert(await(fut) == 1)
        assert(runs == 1)
      }
    "returns to executor from nested fork" in
      withScheduler() { s =>
        val exec = Executors.newCachedThreadPool()
        val t1 = Thread.currentThread()
        try {
          val fut =
            s.fork(exec) {
              val t2 = Thread.currentThread()
              assert(t1 != t2)
              s.fork {
                  assert(t1 != Thread.currentThread())
                  assert(t2 != Thread.currentThread())
                  Future.value(1)
                }.map { i =>
                  assert(t2 == Thread.currentThread())
                  i + 1
                }
            }
          assert(await(fut) == 2)
        } finally {
          exec.shutdown()
        }
      }
  }

  "withMaxSyncConcurrency" - {
    "concurrency and max waiters need to be > 0" in withScheduler(threads = 10) { s =>
      def test(concurrency: Int, maxWaiters: Int) =
        assert(Try(s.withMaxSyncConcurrency(concurrency, maxWaiters)).isThrow)
      test(0, 0)
      test(0, 10)
      test(10, 0)
      test(-1, 0)
      test(-1, -1)
      test(-1, 10)
      test(10, -1)
    }
    "doesn't limit asynchronous concurrency" in withScheduler(threads = 10) { s =>
      val ms = s.withMaxSyncConcurrency(2, Int.MaxValue)
      val running = new AtomicInteger()
      def task(i: Int) = {
        val p = Promise[Unit]
        val fut =
          ms.fork {
            running.incrementAndGet()
            p.map(_ => i).ensure {
              running.decrementAndGet()
            }
          }
        (fut, p)
      }
      val (f1, p1) = task(1)
      val (f2, p2) = task(2)
      val (f3, p3) = task(3)
      val (f4, p4) = task(4)
      eventually(running.get() == 4)
      p1.setDone()
      assert(await(f1) == 1)
      eventually(running.get() == 3)
      p2.setDone()
      p3.setDone()
      p4.setDone()
      assert(await(f2) == 2)
      assert(await(f3) == 3)
      assert(await(f4) == 4)
      eventually(running.get() == 0)
    }
    "limits synchronous concurrency" in withScheduler(threads = 10) { s =>
      val running = new AtomicInteger
      val ms = s.withMaxSyncConcurrency(2, Int.MaxValue)
      def task(i: Int) = {
        val cdl = CountDownLatch(1)
        val fut =
          ms.fork {
            running.incrementAndGet()
            cdl.await()
            running.decrementAndGet()
            Future.value(i)
          }
        (fut, cdl)
      }
      val (f1, cdl1) = task(1)
      val (f2, cdl2) = task(2)
      val (f3, cdl3) = task(3)
      val (f4, cdl4) = task(4)
      eventually(running.get() == 2)
      cdl1.countDown()
      assert(await(f1) == 1)
      eventually(running.get() == 2)
      cdl2.countDown()
      cdl3.countDown()
      assert(await(f2) == 2)
      assert(await(f3) == 3)
      eventually(running.get() == 1)
      cdl4.countDown()
      assert(await(f4) == 4)
      eventually(running.get() == 0)
    }
  }

  "asExecutorService" - {
    "executes task" in
      withScheduler() { s =>
        val exec = s.asExecutorService()
        @volatile var executed = 0
        exec.execute(() => executed += 1)
        eventually(executed == 1)
      }
    "rejects tasks after shutdown" in
      withScheduler() { s =>
        val exec = s.asExecutorService()
        @volatile var executed = 0
        exec.shutdown()
        val t = Try(exec.execute(() => executed += 1))
        assert(t.isThrow)
        assert(t.throwable.isInstanceOf[RejectedExecutionException])
        assert(executed == 0)
      }
  }

  "blocking deadlock bug" - {
    "thread local fiber" in {
      withScheduler(threads = 1, adaptive = false) { s =>
        val p = Promise[Int]
        s.submit { () =>
          s.submit(() => p.setValue(1))
          awaitReady(p)
        }
        await(p)
      }
    }
    "forked fiber" in {
      withScheduler(threads = 2, adaptive = false) { s =>
        val p = Promise[Int]
        s.fork {
          s.submit(() => p.setValue(1))
          awaitReady(p)
        }
        await(p)
      }
    }
  }

  def withScheduler(
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
