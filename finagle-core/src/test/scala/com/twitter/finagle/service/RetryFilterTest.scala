package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{Stat, StatsReceiver}
import com.twitter.finagle.{MockTimer, Service, WriteException}
import com.twitter.util._
import org.junit.runner.RunWith
import org.mockito.Mockito.{never, times, verify, when}
import org.mockito.Matchers.anyObject
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class RetryFilterTest extends FunSpec with MockitoSugar {
  val timer = new MockTimer
  val backoffs = Stream(1.second, 2.seconds, 3.seconds)
  val shouldRetryException: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(WriteException(_)) => true
    case _ => false
  }

  val goodResponse = 321
  val badResponse = 111

  val idempotentRequest = 123
  val nonIdempotentRequest = 999

  val shouldRetryResponse: PartialFunction[(Int, Try[Int]), Boolean] = {
    case (`idempotentRequest`, Throw(WriteException(_))) => true
    case (`idempotentRequest`, Return(`badResponse`)) => true
    case _ => false
  }

  val exceptionOnlyRetryPolicy = RetryPolicy.tries(3, shouldRetryException)
  val retryPolicy = RetryPolicy.tries(3, shouldRetryResponse)

  class TriesFixture(retryExceptionsOnly: Boolean) {
    val stats = mock[StatsReceiver]
    val retriesStat = mock[Stat]
    when(stats.stat("retries")) thenReturn retriesStat
    val service = mock[Service[Int, Int]]
    when(service.close(anyObject[Time])) thenReturn Future.Done
    val filter = if (retryExceptionsOnly)
        new RetryExceptionsFilter[Int, Int](RetryPolicy.tries(3, shouldRetryException), timer, stats)
      else
        new RetryFilter[Int, Int](RetryPolicy.tries(3, shouldRetryResponse), timer, stats)
    val retryingService = filter andThen service
  }

  class PolicyFixture(policy: RetryPolicy[_], retryExceptionsOnly: Boolean) {
    val stats = mock[StatsReceiver]
    val retriesStat = mock[Stat]
    when(stats.stat("retries")) thenReturn retriesStat
    val filter = if (retryExceptionsOnly)
        new RetryExceptionsFilter[Int, Int](policy.asInstanceOf[RetryPolicy[Try[Nothing]]], timer, stats)
      else
        new RetryFilter[Int, Int](policy.asInstanceOf[RetryPolicy[(Int, Try[Int])]], timer, stats)
    val service = mock[Service[Int, Int]]
    when(service.close(anyObject[Time])) thenReturn Future.Done
    val retryingService = filter andThen service
  }

  describe("RetryFilter") {

    describe("with RetryPolicy.tries") {

      def runWithTries(retryExceptionsOnly: Boolean) {
        it("always try once") {
          new TriesFixture(retryExceptionsOnly) {
            when(service(123)) thenReturn Future(321)
            assert(Await.result(retryingService(123)) === 321)
            verify(service)(123)
          }
        }

        it("when failing with WriteExceptions, retry n-1 times") {
          new TriesFixture(retryExceptionsOnly){
            when(service(123)) thenReturn Future.exception(WriteException(new Exception))
            val f = retryingService(123)
            intercept[WriteException] {
              Await.result(f)
            }
            verify(service, times(3))(123)
          }
        }

        it("when failed with a non-WriteException, fail immediately") {
          new TriesFixture(retryExceptionsOnly) {
            when(service(123)) thenReturn Future.exception(new Exception("WTF!"))
            val e = intercept[Exception] {
              Await.result(retryingService(123))
            }
            assert(e.getMessage === "WTF!")
            verify(service)(123)
            verify(retriesStat).add(0)
          }
        }

        it("when no retry occurs, no stat update") {
          new TriesFixture(retryExceptionsOnly) {
            when(service(123)) thenReturn Future(goodResponse)
            assert(Await.result(retryingService(123)) === goodResponse)
            verify(retriesStat).add(0)
          }
        }

        it("propagate interrupts") {
          new TriesFixture(retryExceptionsOnly) {
            val replyPromise = new Promise[Int] {
              @volatile var interrupted: Option[Throwable] = None
              setInterruptHandler { case exc => interrupted = Some(exc) }
            }
            when(service(123)) thenReturn replyPromise

            val res = retryingService(123)
            assert(res.isDefined === false)
            assert(replyPromise.interrupted === None)

            val exc = new Exception
            res.raise(exc)
            assert(res.isDefined === false)
            assert(replyPromise.interrupted === Some(exc))
          }
        }
      }

      describe("using RetryPolicy[(Req, Try[Rep])]") {
        runWithTries(retryExceptionsOnly=false)

        it("when failing with WriteExceptions and non-idempotent request, don't retry") {
          new TriesFixture(retryExceptionsOnly=false){
            when(service(nonIdempotentRequest)) thenReturn Future.exception(WriteException(new Exception))
            val f = retryingService(nonIdempotentRequest)
            intercept[WriteException] {
              Await.result(f)
            }
            verify(service, times(1))(nonIdempotentRequest)
          }
        }

        it("when succeeds with bad response and idempotent request, retry n-1 times") {
          new TriesFixture(retryExceptionsOnly=false) {
            when(service(idempotentRequest)) thenReturn Future(badResponse)
            val f = retryingService(idempotentRequest)
            Await.result(f)
            verify(service, times(3))(idempotentRequest)
          }
        }

        it("when succeeds with bad response and non-idempotent request, don't retry") {
          new TriesFixture(retryExceptionsOnly=false) {
            when(service(nonIdempotentRequest)) thenReturn Future(badResponse)
            val f = retryingService(nonIdempotentRequest)
            Await.result(f)
            verify(service, times(1))(nonIdempotentRequest)
          }
        }
      }

      describe("using RetryPolicy[Try[Nothing]]") {
        runWithTries(retryExceptionsOnly=true)
      }
    }

    describe("with RetryPolicy.backoff: Exception cases") {
      describe("using RetryPolicy[(Req, Try[Rep])]") {
        testExceptionPolicy(RetryPolicy.backoff(backoffs)(shouldRetryResponse), retryExceptionsOnly=false)
      }
      describe("using RetryPolicy[Try[Nothing]]") {
        testExceptionPolicy(RetryPolicy.backoff(backoffs)(shouldRetryException), retryExceptionsOnly=true)
      }
    }

    describe("with RetryPolicy.backoffJava: Exception cases") {
      describe("using RetryPolicy[(Req, Try[Rep])]") {
        testExceptionPolicy(RetryPolicy.backoffJava(Backoff.toJava(backoffs), shouldRetryResponse), retryExceptionsOnly=false)
      }
      describe("using RetryPolicy[Try[Nothing]]") {
        testExceptionPolicy(RetryPolicy.backoffJava(Backoff.toJava(backoffs), shouldRetryException), retryExceptionsOnly=true)
      }
    }

    describe("with Success RetryPolicy.backoff") {
      testSuccessPolicy(RetryPolicy.backoff(backoffs)(shouldRetryResponse))
    }

    describe("with Success RetryPolicy.backoffJava") {
      testSuccessPolicy(RetryPolicy.backoffJava(Backoff.toJava(backoffs), shouldRetryResponse))
    }

    def testExceptionPolicy(policy: RetryPolicy[_], retryExceptionsOnly: Boolean) {

      it("always try once") {
        new PolicyFixture(policy, retryExceptionsOnly) {
          when(service(123)) thenReturn Future(321)
          assert(Await.result(retryingService(123)) === 321)
          verify(service)(123)
          verify(retriesStat).add(0)
        }
      }

      it("when failed with a WriteException, consult the retry strategy") {
        new PolicyFixture(policy, retryExceptionsOnly) {
          Time.withCurrentTimeFrozen { tc =>
            when(service(123)) thenReturn Future.exception(WriteException(new Exception))
            val f = retryingService(123)
            verify(service)(123)
            assert(f.isDefined === false)
            assert(timer.tasks.size === 1)

            when(service(123)) thenReturn Future(321) // we succeed next time; tick!
            tc.advance(1.second); timer.tick()

            verify(service, times(2))(123)
            verify(retriesStat).add(1)
            assert(Await.result(f) === 321)
          }
        }
      }

      it("give up when the retry strategy is exhausted") {
        new PolicyFixture(policy, retryExceptionsOnly) {
          Time.withCurrentTimeFrozen { tc =>
            when(service(123)) thenReturn Future.exception(WriteException(new Exception("i'm exhausted")))
            val f = retryingService(123)
            1 to 3 foreach { i =>
              assert(f.isDefined === false)
              verify(service, times(i))(123)
              verify(retriesStat, never).add(3)
              tc.advance(i.seconds); timer.tick()
            }

            verify(retriesStat).add(3)
            assert(f.isDefined === true)
            assert(Await.ready(f).poll.get.isThrow === true)
            val e = intercept[WriteException] {
              Await.result(f)
            }
            assert(e.getMessage.endsWith("i'm exhausted") === true)
          }
        }
      }

      it("when failed with a non-WriteException, fail immediately") {
        new PolicyFixture(policy, retryExceptionsOnly) {
          when(service(123)) thenReturn Future.exception(new Exception("WTF!"))
          val e = intercept[Exception] {
            Await.result(retryingService(123))
          }
          assert(e.getMessage === "WTF!")
          verify(service)(123)
          assert(timer.tasks.isEmpty === true)
          verify(retriesStat).add(0)
        }
      }

      it("when no retry occurs, no stat update") {
        new PolicyFixture(policy, retryExceptionsOnly) {
          when(service(123)) thenReturn Future(321)
          assert(Await.result(retryingService(123)) === 321)
          verify(retriesStat).add(0)
        }
      }

      it("propagate cancellation") {
        new PolicyFixture(policy, retryExceptionsOnly) {
          val replyPromise = new Promise[Int] {
            @volatile var interrupted: Option[Throwable] = None
            setInterruptHandler { case exc => interrupted = Some(exc) }
          }
          when(service(123)) thenReturn replyPromise

          val res = retryingService(123)
          assert(res.isDefined === false)
          assert(replyPromise.interrupted === None)

          val exc = new Exception
          res.raise(exc)
          assert(res.isDefined === false)
          assert(replyPromise.interrupted === Some(exc))
        }
      }
    }

    def testSuccessPolicy(policy: RetryPolicy[(Int, Try[Int])]) {

      it("when it succeeds with a bad response, consult the retry strategy") {
        new PolicyFixture(policy, retryExceptionsOnly=false) {
          Time.withCurrentTimeFrozen { tc =>
            when(service(123)) thenReturn Future(badResponse)
            val f = retryingService(123)
            verify(service)(123)
            assert(f.isDefined === false)
            assert(timer.tasks.size === 1)

            when(service(123)) thenReturn Future(goodResponse) // we succeed next time; tick!
            tc.advance(1.second); timer.tick()

            verify(service, times(2))(123)
            verify(retriesStat).add(1)
            assert(Await.result(f) === goodResponse)
          }
        }
      }

      it("return result when the retry strategy is exhausted") {
        new PolicyFixture(policy, retryExceptionsOnly=false) {
          Time.withCurrentTimeFrozen { tc =>
            when(service(123)) thenReturn Future(badResponse)
            val f = retryingService(123)
            1 to 3 foreach { i =>
              assert(f.isDefined === false)
              verify(service, times(i))(123)
              verify(retriesStat, never).add(3)
              tc.advance(i.seconds); timer.tick()
            }

            verify(retriesStat).add(3)
            assert(f.isDefined === true)
            assert(Await.result(f) === badResponse)
          }
        }
      }

      it("when it succeeds, return the result immediately") {
        new PolicyFixture(policy, retryExceptionsOnly=false) {
          when(service(123)) thenReturn Future(goodResponse)
          val f = retryingService(123)
          verify(service)(123)
          assert(timer.tasks.isEmpty === true)
          verify(retriesStat).add(0)
        }
      }

     it("when no retry occurs, no stat update") {
        new PolicyFixture(policy, retryExceptionsOnly=false) {
          when(service(123)) thenReturn Future(321)
          assert(Await.result(retryingService(123)) === 321)
          verify(retriesStat).add(0)
        }
      }
    }
  }

  describe("Backoff") {
    it("Backoff.exponential") {
      val backoffs = Backoff.exponential(1.seconds, 2) take 10
      assert(backoffs.force.toSeq === (0 until 10 map { i => (1 << i).seconds }))
    }

    it("Backoff.exponential with upper limit") {
      val backoffs = (Backoff.exponential(1.seconds, 2) take 5) ++ Backoff.const(32.seconds)
      assert((backoffs take 10).force.toSeq === (0 until 10 map {
        i => math.min(1 << i, 32).seconds
      }))
    }

    it("Backoff.linear") {
      val backoffs = Backoff.linear(2.seconds, 10.seconds) take 10
      assert(backoffs.head === 2.seconds)
      assert(backoffs.tail.force.toSeq === (1 until 10 map { i => 2.seconds + 10.seconds * i }))
    }

    it("Backoff.const") {
      val backoffs = Backoff.const(10.seconds) take 10
      assert(backoffs.force.toSeq === (0 until 10 map { _ => 10.seconds}))
    }
  }
}
