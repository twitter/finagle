package com.twitter.finagle.service

import java.util.concurrent.atomic.AtomicInteger
import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{Status, FailedFastException, MockTimer, Service, ServiceFactory, SourcedException}
import com.twitter.util._
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.concurrent.Conductors
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class FailFastFactoryTest extends FunSuite with MockitoSugar with Conductors {

  def newCtx() = new {
    val timer = new MockTimer
    val backoffs = 1.second #:: 2.seconds #:: Stream.empty[Duration]
    val service = mock[Service[Int, Int]]
    when(service.close(any[Time])).thenReturn(Future.Done)
    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.status).thenReturn(Status.Open)
    when(underlying.close(any[Time])).thenReturn(Future.Done)
    val stats = new InMemoryStatsReceiver
    val failfast = new FailFastFactory(underlying, stats, timer, backoffs)

    val p, q, r = new Promise[Service[Int, Int]]
    when(underlying()).thenReturn(p)
    val pp = failfast()
    assert(pp.isDefined === false)
    assert(failfast.isAvailable === true, s"Failfast status: ${failfast.status}")
    assert(timer.tasks.isEmpty)
  }

  test("pass through whenever everything is fine") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      p() = Return(service)
      assert(pp.poll === Some(Return(service)))
    }
  }

  test("failure") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      p() = Throw(new Exception)
      verify(underlying).apply()
      assert(failfast.isAvailable === false)
      assert(stats.counters.get(Seq("marked_dead")) === Some(1))
    }
  }

  test("time out according to backoffs") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._
      p() = Throw(new Exception)

      assert(timer.tasks.size === 1)
      tc.set(timer.tasks(0).when)
      timer.tick()
      verify(underlying, times(2)).apply()
      assert(failfast.isAvailable === false)
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
      timer.tick()
      verify(underlying, times(2)).apply()
      assert(timer.tasks.isEmpty)
      q() = Return(service)
      assert(timer.tasks.isEmpty)
      assert(failfast.isAvailable === true)
      assert(stats.counters.get(Seq("marked_available")) === Some(1))
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
      assert(failfast().poll === None)
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

      assert(timer.tasks.size === 1)
      assert(failfast.isAvailable === false)
      verify(underlying, never()).close()
      failfast.close()
      verify(underlying).close()
      assert(timer.tasks.isEmpty)
      assert(failfast.status === underlying.status)

      val status = underlying.status match {
        case Status.Open => Status.Closed
        case Status.Closed => Status.Open
        case status => fail(s"bad status $status")
      }
      when(underlying.status).thenReturn(status)
      assert(failfast.status === underlying.status)
    }
  }

  test("fails simultaneous requests properly") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      val pp2 = failfast()
      val e = new Exception
      p() = Throw(e)

      assert(pp.poll === Some(Throw(e)))
      assert(pp2.poll === Some(Throw(e)))

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

      thread("threadOne") {
        val ctx = newCtx()
        ctx.p() = Throw(new Exception)
        ctx.failfast().poll match {
          case Some(Throw(ex: FailedFastException)) => {
            ex.serviceName = "threadOne"
            assert(beat === 0)
          }
          case _ => throw new Exception
        }
        threadCompletionCount.incrementAndGet()
      }

      thread("threadTwo") {
        waitForBeat(1)
        val ctx = newCtx()
        ctx.p() = Throw(new Exception)
        ctx.failfast().poll match {
          case Some(Throw(ex: FailedFastException)) => {
            assert(ex.serviceName === SourcedException.UnspecifiedServiceName)
          }
          case _ => throw new Exception
        }
        threadCompletionCount.incrementAndGet()
      }

      whenFinished {
        assert(threadCompletionCount.get === 2)
      }
    }
  }

  test("accepts empty backoff stream") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = newCtx()
      import ctx._

      val failfast = new FailFastFactory(underlying, stats, timer, Stream.empty)
      failfast()
    }
  }
}
