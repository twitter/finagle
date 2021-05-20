package com.twitter.finagle.server

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future, FuturePool, MockTimer, Time, TimeControl}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.scalactic.Tolerance._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ThreadUsageTest extends AnyFunSuite with BeforeAndAfter {

  private[this] val epsilon = 1e-3

  private[this] val stats = new InMemoryStatsReceiver()
  private[this] var timer = new MockTimer()
  private[this] var usage = new ThreadUsage(stats, timer)

  before {
    stats.clear()
    timer = new MockTimer()
    usage = new ThreadUsage(stats, timer)
  }

  private def perThreadMean: Double =
    stats.gauges(Seq("mean"))()

  private def perThreadStddev: Double =
    stats.gauges(Seq("stddev"))()

  private def relativeStddev: Double =
    stats.gauges(Seq("relative_stddev"))()

  private def triggerComputation(tc: TimeControl): Unit = {
    tc.advance(60.seconds)
    timer.tick()
  }

  test("initial metrics state") {
    assert(0.0 == perThreadMean)
    assert(0.0 == perThreadStddev)
    assert(0.0 == relativeStddev)
  }

  test("single thread basics") {
    Time.withCurrentTimeFrozen { tc =>
      usage.increment()
      triggerComputation(tc)
      assert(1.0 == perThreadMean)
      assert(0.0 == perThreadStddev)
      assert(0.0 == relativeStddev)

      // ensure that we only see the last window's data
      triggerComputation(tc)
      assert(0.0 == perThreadMean)
      assert(0.0 == perThreadStddev)
      assert(0.0 == relativeStddev)
    }
  }

  private def incrsPerThread(counts: Int*): Future[Unit] = {
    // use a pool and a latch to let each thread's increments happen
    // to happen in its own thread.
    val latch = new CountDownLatch(counts.size)

    Future
      .collect(counts.map { count =>
        FuturePool.unboundedPool {
          latch.countDown()
          if (!latch.await(5, TimeUnit.SECONDS)) {
            Future.exception(new Exception("never latched"))
          } else {
            0.until(count).foreach { _ => usage.increment() }
            Future.Done
          }
        }
      })
      .unit
  }

  test("basics") {
    Time.withCurrentTimeFrozen { tc =>
      Await.result(incrsPerThread(100, 100, 100), 5.seconds)
      triggerComputation(tc)
      assert(100.0 == perThreadMean)
      assert(0.0 == perThreadStddev)
      assert(0.0 == relativeStddev)

      Await.result(incrsPerThread(90, 100, 110), 5.seconds)
      triggerComputation(tc)
      assert(100.0 == perThreadMean)
      assert(8.165 === perThreadStddev +- epsilon)
      assert(0.0816 === relativeStddev +- epsilon)

      Await.result(incrsPerThread(1, 5, 6, 8, 10, 40, 65, 88), 5.seconds)
      triggerComputation(tc)
      assert(27.875 === perThreadMean +- epsilon)
      assert(30.779 === perThreadStddev +- epsilon)
      assert(1.104 === relativeStddev +- epsilon)

      Await.result(incrsPerThread(1, 1, 1, 100), 5.seconds)
      triggerComputation(tc)
      assert(25.75 === perThreadMean +- epsilon)
      assert(42.868 === perThreadStddev +- epsilon)
      assert(1.665 === relativeStddev +- epsilon)
    }
  }

}
