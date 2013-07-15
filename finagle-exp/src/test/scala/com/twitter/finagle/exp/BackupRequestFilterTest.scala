package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Service, MockTimer, BackupRequestLost}
import com.twitter.util.{Future, Promise, Time, Return, TimeControl, Duration, Stopwatch}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BackupRequestFilterTest extends FunSuite with MockitoSugar {
  object TimeStopwatch extends Stopwatch {
    def start() = {
      val begin = Time.now
      () => Time.now - begin
    }
  }

  def quantile(ds: Seq[Duration], which: Int) = {
    val sorted = ds.sorted
    sorted(which*sorted.size/100)
  }
  
  def newCtx() = new {
    val range = 10.seconds
    val timer = new MockTimer
    val statsReceiver = new InMemoryStatsReceiver
    val underlying = mock[Service[String, String]]
    when(underlying.close(anyObject())) thenReturn Future.Done
    val filter = new BackupRequestFilter[String, String](
      95, range, timer, statsReceiver, 10.seconds, TimeStopwatch)
    val service = filter andThen underlying
    val cutoffGauge = statsReceiver.gauges(Seq("cutoff_ms"))
    def cutoff() = Duration.fromMilliseconds(cutoffGauge().toInt)
    val rng = new Random(123)
    val latencies = Seq.fill(100) {
      Duration.fromMilliseconds(rng.nextInt).abs % (range/2)
    }
  }

  test("maintain cutoffs") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      for ((l, i) <- latencies.zipWithIndex) {
        val p = new Promise[String]
        when(underlying("ok")) thenReturn p
        verify(underlying, times(i)).apply("ok")
        val f = service("ok")
        assert(!f.isDefined)
        tc.advance(l)
        p.setValue("ok")
        assert(f.poll === Some(Return("ok")))
        assert(quantile(latencies take i+1, 95) === cutoff())
      }
    }
  }

  test("issue backup request") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._
      
      for (l <- latencies) {
        val p = new Promise[String]
        when(underlying("ok")) thenReturn p
        val f = service("ok")
        tc.advance(l)
        p.setValue("ok")
      }

      // flush
      timer.tick()
      assert(timer.tasks.isEmpty)
      assert(statsReceiver.counters(Seq("won")) === latencies.size)
      assert(cutoff() > Duration.Zero)
      
      val p = new Promise[String]
      when(underlying("a")) thenReturn p
      verify(underlying, times(0)).apply("a")
      
      val f = service("a")
      verify(underlying).apply("a")
      assert(!f.isDefined)
      assert(timer.tasks.size === 1)
      
      tc.advance(cutoff()/2)
      timer.tick()
      assert(timer.tasks.size === 1)
      verify(underlying).apply("a")
      val p1 = new Promise[String]
      when(underlying("a")) thenReturn p1
      tc.advance(cutoff()/2)
      timer.tick()
      assert(timer.tasks.isEmpty)
      verify(underlying, times(2)).apply("a")

      p1.setValue("backup")
      assert(f.poll === Some(Return("backup")))
      assert(p1.isInterrupted === None)
      assert(p.isInterrupted === Some(BackupRequestLost))
      assert(statsReceiver.counters(Seq("lost")) === 1)
    }
  }
  
  test("cancels backup when original wins") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      for (l <- latencies) {
        val p = new Promise[String]
        when(underlying("ok")) thenReturn p
        val f = service("ok")
        tc.advance(l)
        p.setValue("ok")
      }

      // flush
      timer.tick()
      assert(timer.tasks.isEmpty)
      assert(statsReceiver.counters(Seq("won")) === latencies.size)
      assert(cutoff() > Duration.Zero)

      val p = new Promise[String]
      when(underlying("b")) thenReturn p
      val f = service("b")
      verify(underlying).apply("b")
      assert(timer.tasks.size === 1)
      val task = timer.tasks(0)
      assert(!task.isCancelled)
      tc.advance(cutoff()/2)
      assert(timer.tasks.toSeq === Seq(task))
      verify(underlying).apply("b")
      assert(!task.isCancelled)
      p.setValue("orig")
      assert(task.isCancelled)
      timer.tick()
      assert(timer.tasks.isEmpty)
    }
  }
}
