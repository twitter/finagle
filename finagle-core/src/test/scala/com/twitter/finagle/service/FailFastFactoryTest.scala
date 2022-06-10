package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.Backoff
import com.twitter.finagle.FailedFastException
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SourcedException
import com.twitter.finagle.Status
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Matchers.any
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.concurrent.Conductors
import org.scalatest.concurrent.IntegrationPatience
import org.scalatestplus.mockito.MockitoSugar
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class FailFastFactoryTest
    extends AnyFunSuite
    with MockitoSugar
    with Conductors
    with IntegrationPatience {

  def newCtx() = new {
    val timer = new MockTimer
    val backoffs = Backoff.linear(1.second, 1.second).take(2)
    val service = mock[Service[Int, Int]]
    when(service.close(any[Time])).thenReturn(Future.Done)
    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.status).thenReturn(Status.Open)
    when(underlying.close(any[Time])).thenReturn(Future.Done)
    val stats = new InMemoryStatsReceiver
    val label = "test"
    val failfast = new FailFastFactory(underlying, stats, timer, label, backoffs = backoffs)

    val p, q, r = new Promise[Service[Int, Int]]
    when(underlying()).thenReturn(p)
    val pp = failfast()
    assert(!pp.isDefined)
    assert(failfast.isAvailable)
    assert(timer.tasks.isEmpty)
  }

  test("pass through whenever everything is fine") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      p() = Return(service)
      assert(pp.poll.contains(Return(service)))
    }
  }

  test("failure") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      val e = new Exception("boom")
      p() = Throw(e)
      val f = verify(underlying).apply()

      assert(failfast().poll.map(_.throwable.getCause).contains(e))
      assert(!failfast.isAvailable)
      assert(stats.counters.get(Seq("marked_dead")).contains(1))
      assert(stats.gauges(Seq("is_marked_dead"))() == 1)
    }
  }

  test("time out according to backoffs") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._
      p() = Throw(new Exception)

      assert(timer.tasks.size == 1)
      tc.set(timer.tasks(0).when)
      timer.tick()
      verify(underlying, times(2)).apply()
      assert(!failfast.isAvailable)
    }
  }

  test("become available again if the next attempt succeeds") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._
      p() = Throw(new Exception)

      tc.set(timer.tasks(0).when)
      when(underlying()).thenReturn(q)
      verify(underlying).apply()
      assert(stats.counters.get(Seq("marked_dead")).contains(1))
      assert(stats.gauges(Seq("is_marked_dead"))() == 1)
      timer.tick()
      verify(underlying, times(2)).apply()
      assert(timer.tasks.isEmpty)
      q() = Return(service)
      assert(timer.tasks.isEmpty)
      assert(failfast.isAvailable)
      assert(stats.counters.get(Seq("marked_available")) == Some(1))
      assert(stats.gauges(Seq("is_marked_dead"))() == 0)
    }
  }

  test("is Busy when failing; done when revived") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      assert(failfast.status == Status.Open)
      p() = Throw(new Exception)
      assert(failfast.status == Status.Busy)

      tc.set(timer.tasks(0).when)
      when(underlying()).thenReturn(Future.value(service))
      timer.tick()
      assert(failfast.status == Status.Open)
    }
  }

  test("refuse external attempts") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._
      p() = Throw(new Exception)

      assert {
        failfast().poll match {
          case Some(Throw(_: FailedFastException)) => true
          case _ => false
        }
      }
      verify(underlying).apply() // nothing new
    }
  }

  test("admit external attempts when available again") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._
      p() = Throw(new Exception)

      tc.set(timer.tasks(0).when)
      verify(underlying).apply()
      when(underlying()).thenReturn(q)
      timer.tick()
      verify(underlying, times(2)).apply()
      q() = Return(service)
      when(underlying()).thenReturn(r)
      assert(failfast().poll == None)
      r() = Return(service)
      assert {
        failfast().poll match {
          case Some(Return(s)) => s eq service
          case _ => false
        }
      }
    }
  }

  test("cancels timer on close") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._
      p() = Throw(new Exception)

      assert(timer.tasks.size == 1)
      assert(!failfast.isAvailable)
      verify(underlying, never()).close()
      failfast.close()
      verify(underlying).close()
      assert(timer.tasks.isEmpty)
      assert(failfast.status == underlying.status)

      val status = underlying.status match {
        case Status.Open => Status.Closed
        case Status.Closed => Status.Open
        case status => fail(s"bad status $status")
      }
      when(underlying.status).thenReturn(status)
      assert(failfast.status == underlying.status)
    }
  }

  test("fails simultaneous requests properly") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      val pp2 = failfast()
      val e = new Exception
      p() = Throw(e)

      assert(pp.poll == Some(Throw(e)))
      assert(pp2.poll == Some(Throw(e)))

      val ffe = intercept[FailedFastException] {
        failfast().poll.get.get
      }
      assert(ffe.getMessage().contains("twitter.github.io/finagle/guide/FAQ.html"))
    }
  }

  test("maintains separate exception state in separate threads") {
    Time.withCurrentTimeFrozen { tc =>
      val conductor = new Conductor
      import conductor._

      val threadCompletionCount = new AtomicInteger(0)

      threadNamed("threadOne") {
        val ctx = newCtx()
        ctx.p() = Throw(new Exception)
        ctx.failfast().poll match {
          case Some(Throw(ex: FailedFastException)) => {
            ex.serviceName = "threadOne"
            assert(beat == 0)
          }
          case _ => throw new Exception
        }
        threadCompletionCount.incrementAndGet()
      }

      threadNamed("threadTwo") {
        waitForBeat(1)
        val ctx = newCtx()
        ctx.p() = Throw(new Exception)
        ctx.failfast().poll match {
          case Some(Throw(ex: FailedFastException)) => {
            assert(ex.serviceName == SourcedException.UnspecifiedServiceName)
          }
          case _ => throw new Exception
        }
        threadCompletionCount.incrementAndGet()
      }

      whenFinished {
        assert(threadCompletionCount.get == 2)
      }
    }
  }

  test("accepts empty backoff stream") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      val failfast = new FailFastFactory(underlying, stats, timer, label, backoffs = Backoff.empty)
      failfast()
    }
  }
}
