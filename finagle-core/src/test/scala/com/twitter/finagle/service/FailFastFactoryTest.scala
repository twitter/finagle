package com.twitter.finagle.service

import com.twitter.finagle.{FailedFastException, ServiceFactory, Service}
import com.twitter.util._
import com.twitter.finagle.MockTimer
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.conversions.time._
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, verify, when, times}
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class FailFastFactoryTest extends FunSuite with MockitoSugar {

  def newCtx() = new {
    val timer = new MockTimer
    val backoffs = 1.second #:: 2.seconds #:: Stream.empty[Duration]
    val service = mock[Service[Int, Int]]
    when(service.close(any[Time])).thenReturn(Future.Done)
    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.isAvailable).thenReturn(true)
    when(underlying.close(any[Time])).thenReturn(Future.Done)
    val failfast = new FailFastFactory(underlying, NullStatsReceiver, timer, backoffs)

    val p, q, r = new Promise[Service[Int, Int]]
    when(underlying()).thenReturn(p)
    val pp = failfast()
    assert(pp.isDefined === false)
    assert(failfast.isAvailable === true)
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
      verify(underlying).apply()  // nothing new
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
      assert(failfast.isAvailable === underlying.isAvailable)
      val ia = !underlying.isAvailable
      when(underlying.isAvailable).thenReturn(ia)
      assert(failfast.isAvailable === underlying.isAvailable)
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

      intercept[FailedFastException] {
        failfast().poll.get.get
      }
    }
  }
}
