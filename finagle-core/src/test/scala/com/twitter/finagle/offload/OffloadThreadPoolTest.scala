package com.twitter.finagle.offload

import com.twitter.finagle.stats.InMemoryStatsReceiver
import java.util.concurrent.{Callable, CountDownLatch}
import org.scalatest.funsuite.AnyFunSuite

class OffloadThreadPoolTest extends AnyFunSuite {
  test("tasks that overflow the queue are executed by the submitting thread") {
    val stats = new InMemoryStatsReceiver
    val pool = OffloadThreadPool(1, 1, stats)
    val initialLatch = new CountDownLatch(1)

    // block the only worker we have until the test is over
    pool.submit(new Runnable {
      def run(): Unit = initialLatch.await()
    })

    // Take up a slot in the queue
    pool.submit(new Runnable {
      def run(): Unit = ()
    })
    assert(stats.counters(Seq("not_offloaded_tasks")) == 0)

    // Submit a task for rejection
    val threadFuture = pool.submit(new Callable[Thread] {
      def call() = Thread.currentThread()
    })

    assert(threadFuture.get() eq Thread.currentThread())
    assert(stats.counters(Seq("not_offloaded_tasks")) == 1)
    initialLatch.countDown()
    pool.shutdown()
  }
}
