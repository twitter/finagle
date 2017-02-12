package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{Service, BackupRequestLost}
import com.twitter.util._
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BackupRequestFilterTest extends FunSuite
  with MockitoSugar
  with Matchers {
  def quantile(ds: Seq[Duration], which: Int) = {
    val sorted = ds.sorted
    sorted(which * sorted.size / 100)
  }

  def newCtx() = new {
    val maxDuration = 10.seconds
    val timer = new MockTimer
    val statsReceiver = new InMemoryStatsReceiver
    val underlying = mock[Service[String, String]]
    when(underlying.close(anyObject())).thenReturn(Future.Done)
    val filter = new BackupRequestFilter[String, String](
      95, maxDuration, timer, statsReceiver, Duration.Top, Stopwatch.timeMillis, 1, 0.05)
    val service = filter andThen underlying

    def cutoff() =
      Duration.fromMilliseconds(filter.cutoffMs())

    val rng = new Random(123)
    val latencies = Seq.fill(100) {
      Duration.fromMilliseconds(rng.nextInt()).abs % (maxDuration / 2)
    }
  }

  test("maintain cutoffs") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      for ((l, i) <- latencies.zipWithIndex) {
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        verify(underlying, times(i)).apply("ok")
        val f = service("ok")
        assert(!f.isDefined)
        tc.advance(l)
        p.setValue("ok")
        assert(f.poll == Some(Return("ok")))
        val ideal = quantile(latencies take i + 1, 95)
        val actual = cutoff()
        BackupRequestFilter.defaultError(maxDuration) match {
          case 0.0 =>
            assert(ideal == actual)
          case error =>
            val epsilon = maxDuration.inMillis * error
            actual.inMillis.toDouble should be(ideal.inMillis.toDouble +- epsilon)
        }
      }
    }
  }

  test("issue backup request") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      for (l <- latencies) {
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        tc.advance(l)
        p.setValue("ok")
      }

      // flush
      timer.tick()
      assert(timer.tasks.isEmpty)
      assert(statsReceiver.counters(Seq("won")) == latencies.size)
      assert(cutoff() > Duration.Zero)

      val p = new Promise[String]
      when(underlying("a")).thenReturn(p)
      verify(underlying, times(0)).apply("a")

      val f = service("a")
      verify(underlying).apply("a")
      assert(!f.isDefined)
      assert(timer.tasks.size == 1)

      tc.advance(cutoff() / 2)
      timer.tick()
      assert(timer.tasks.size == 1)
      verify(underlying).apply("a")
      val p1 = new Promise[String]
      when(underlying("a")).thenReturn(p1)
      tc.advance(cutoff() / 2)
      timer.tick()
      assert(timer.tasks.isEmpty)
      verify(underlying, times(2)).apply("a")

      p1.setValue("backup")
      assert(f.poll == Some(Return("backup")))
      assert(p1.isInterrupted == None)
      assert(p.isInterrupted == Some(BackupRequestLost))
      assert(statsReceiver.counters(Seq("lost")) == 1)
      // original request takes longer than cutoff
      assert(statsReceiver.counters(Seq("timeouts")) == 1)
    }
  }

  test("cancels backup when original wins") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      for (l <- latencies) {
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        tc.advance(l)
        p.setValue("ok")
      }

      // flush
      timer.tick()
      assert(timer.tasks.isEmpty)
      assert(statsReceiver.counters(Seq("won")) == latencies.size)
      assert(cutoff() > Duration.Zero)

      val p = new Promise[String]
      when(underlying("b")).thenReturn(p)
      val f = service("b")
      verify(underlying).apply("b")
      assert(timer.tasks.size == 1)
      val task = timer.tasks(0)
      assert(!task.isCancelled)
      tc.advance(cutoff() / 2)
      assert(timer.tasks.toSeq == Seq(task))
      verify(underlying).apply("b")
      assert(!task.isCancelled)
      p.setValue("orig")
      assert(task.isCancelled)
      timer.tick()
      assert(timer.tasks.isEmpty)
      // original request succeeds within cutoff
      assert(!statsReceiver.counters.contains(Seq("timeouts")))
    }
  }

  test("sends backup request when original fails before backup timer") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      // make a bunch of "requests" that complete before the maxDuration
      for (l <- latencies) {
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        tc.advance(l)
        p.setValue("ok")
      }

      // flush
      timer.tick()
      assert(timer.tasks.isEmpty)
      assert(statsReceiver.counters(Seq("won")) == latencies.size)
      assert(cutoff() > Duration.Zero)

      val origPromise = new Promise[String]
      origPromise.setInterruptHandler { case t => origPromise.updateIfEmpty(Throw(t)) }
      when(underlying("c")).thenReturn(origPromise)
      verify(underlying, times(0)).apply("c")

      val f = service("c")
      verify(underlying).apply("c")
      assert(!f.isDefined)
      assert(timer.tasks.size == 1) // backup request timer

      tc.advance(cutoff() / 2)
      timer.tick()
      assert(timer.tasks.size == 1) // backup request timer
      verify(underlying).apply("c")
      val backupPromise = new Promise[String]
      when(underlying("c")).thenReturn(backupPromise)

      val cancelEx = new Exception
      origPromise.raise(cancelEx)
      assert(timer.tasks.isEmpty)
      verify(underlying, times(2)).apply("c")

      backupPromise.setValue("backup")
      assert(f.poll == Some(Return("backup")))
      assert(backupPromise.isInterrupted == None)
      val ex = intercept[Exception] { Await.result(origPromise) }
      assert(ex == cancelEx)
      assert(statsReceiver.counters(Seq("lost")) == 1)
      // original request fails instead of timing out
      assert(!statsReceiver.counters.contains(Seq("timeouts")))
    }
  }

  test("return backup request response when original fails after backup is issued") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      // make a bunch of "requests" under the
      for (l <- latencies) {
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        tc.advance(l)
        p.setValue("ok")
      }

      // flush
      timer.tick()
      assert(timer.tasks.isEmpty)
      assert(statsReceiver.counters(Seq("won")) == latencies.size)
      assert(cutoff() > Duration.Zero)

      val origPromise = new Promise[String]
      origPromise.setInterruptHandler { case t => origPromise.updateIfEmpty(Throw(t)) }
      when(underlying("d")).thenReturn(origPromise)
      verify(underlying, times(0)).apply("d")

      val f = service("d")
      verify(underlying).apply("d")
      assert(!f.isDefined)
      assert(timer.tasks.size == 1) // backup request timer

      tc.advance(cutoff() / 2)
      timer.tick()
      assert(timer.tasks.size == 1) // backup request timer
      verify(underlying).apply("d")
      val backupPromise = new Promise[String]
      when(underlying("d")).thenReturn(backupPromise)
      tc.advance(cutoff() / 2)
      timer.tick()
      assert(timer.tasks.isEmpty)
      verify(underlying, times(2)).apply("d")

      val cancelEx = new Exception
      origPromise.raise(cancelEx)

      backupPromise.setValue("backup")
      assert(f.poll == Some(Return("backup")))
      assert(backupPromise.isInterrupted == None)
      val ex = intercept[Exception] { Await.result(origPromise) }
      assert(ex == cancelEx)
      assert(statsReceiver.counters(Seq("lost")) == 1)
      // original request takes longer than cutoff
      assert(statsReceiver.counters(Seq("timeouts")) == 1)
    }
  }
}
