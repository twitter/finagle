package com.twitter.finagle.service

import com.twitter.finagle.{FailedFastException, ServiceFactory, Service}
import com.twitter.util.Promise
import org.specs.mock.Mockito
import org.specs.SpecificationWithJUnit
import com.twitter.finagle.MockTimer
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Future, Time, Return, Throw}
import com.twitter.conversions.time._

class FailFastFactorySpec extends SpecificationWithJUnit with Mockito {
  "a FailFastFactory" should {
    val timer = new MockTimer
    val backoffs = 1.second #:: 2.seconds #:: Stream.empty
    val service = mock[Service[Int, Int]]
    service.close(any) returns Future.Done
    val underlying = mock[ServiceFactory[Int, Int]]
    underlying.isAvailable returns true
    underlying.close(any) returns Future.Done
    val failfast = new FailFastFactory(underlying, NullStatsReceiver, timer, backoffs)

    "pass through whenever everything is fine" in Time.withCurrentTimeFrozen { tc =>
      failfast.isAvailable must beTrue
      val p, q = new Promise[Service[Int, Int]]
      underlying() returns p
      val pp = failfast()
      pp.isDefined must beFalse
      p() = Return(service)
      pp.poll must beSome(Return(service))
    }

    "on failure" in Time.withCurrentTimeFrozen { tc =>
      val p, q, r = new Promise[Service[Int, Int]]
      underlying() returns p
      val pp = failfast()
      pp.isDefined must beFalse
      failfast.isAvailable must beTrue
      timer.tasks must beEmpty
      p() = Throw(new Exception)
      there was one(underlying)()

      "become unavailable" in {
        failfast.isAvailable must beFalse
      }

      "time out according to backoffs" in {
        timer.tasks must haveSize(1)
        tc.set(timer.tasks(0).when)
        timer.tick()
        there were two(underlying)()
        failfast.isAvailable must beFalse
      }

      "become available again if the next attempt succeeds" in {
        tc.set(timer.tasks(0).when)
        there was one(underlying)()
        underlying() returns q
        timer.tick()
        there were two(underlying)()
        timer.tasks must beEmpty
        q() = Return(service)
        timer.tasks must beEmpty
        failfast.isAvailable must beTrue
      }

      "refuse external attempts" in {
        failfast().poll must beLike {
          case Some(Throw(_: FailedFastException)) => true
        }
        there was one(underlying).apply()  // nothing new
      }

      "admit external attempts when available again" in {
        tc.set(timer.tasks(0).when)
        there was one(underlying)()
        underlying() returns q
        timer.tick()
        there were two(underlying)()
        q() = Return(service)
        underlying() returns r
        failfast().poll must beNone
        r() = Return(service)
        failfast().poll must beLike {
          case Some(Return(s)) => s eq service
        }
      }

      "cancels timer on close" in {
        timer.tasks must haveSize(1)
        failfast.isAvailable must beFalse
        there were no(underlying).close()
        failfast.close()
        there was one(underlying).close()
        timer.tasks must beEmpty
        failfast.isAvailable must be_==(underlying.isAvailable)
        val ia = !underlying.isAvailable
        underlying.isAvailable returns ia
        failfast.isAvailable must be_==(underlying.isAvailable)
      }
    }
  }
}
