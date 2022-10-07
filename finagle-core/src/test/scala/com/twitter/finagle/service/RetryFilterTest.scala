package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.Backoff
import com.twitter.finagle.FailedFastException
import com.twitter.finagle.Failure
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Service
import com.twitter.finagle.WriteException
import com.twitter.util._
import org.mockito.ArgumentMatchers.anyObject
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatestplus.mockito.MockitoSugar
import scala.language.reflectiveCalls
import org.scalatest.funspec.AnyFunSpec

class RetryFilterTest extends AnyFunSpec with MockitoSugar with BeforeAndAfter {
  var timer: JavaTimer = _
  val backoffs = Backoff.linear(1.second, 1.second).take(3)
  val shouldRetryException: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(WriteException(_)) => true
    case _ => false
  }

  before {
    timer = new JavaTimer(true)
  }

  after {
    timer.stop()
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
    val stats = new InMemoryStatsReceiver()

    def retriesStat: Seq[Int] = stats.stat("retries")().map(_.toInt)

    val service = mock[Service[Int, Int]]
    when(service.close(anyObject[Time])) thenReturn Future.Done
    val filter =
      if (retryExceptionsOnly)
        new RetryExceptionsFilter[Int, Int](
          RetryPolicy.tries(3, shouldRetryException),
          timer,
          stats
        )
      else
        new RetryFilter[Int, Int](RetryPolicy.tries(3, shouldRetryResponse), timer, stats)
    val retryingService = filter andThen service
  }

  class PolicyFixture(policy: RetryPolicy[_], retryExceptionsOnly: Boolean, theTimer: Timer) {
    val stats = new InMemoryStatsReceiver()
    def retriesStat: Seq[Int] = stats.stat("retries")().map(_.toInt)

    val filter =
      if (retryExceptionsOnly)
        new RetryExceptionsFilter[Int, Int](
          policy.asInstanceOf[RetryPolicy[Try[Nothing]]],
          theTimer,
          stats
        )
      else
        new RetryFilter[Int, Int](
          policy.asInstanceOf[RetryPolicy[(Int, Try[Int])]],
          theTimer,
          stats
        )
    val service = mock[Service[Int, Int]]
    when(service.close(anyObject[Time])) thenReturn Future.Done
    val retryingService = filter andThen service
  }

  describe("RetryFilter") {

    it("respects RetryBudget") {
      val stats = new InMemoryStatsReceiver()

      // use a budget that just gets 2 retries
      val budgetRetries = 2
      val budget = RetryBudget(1.second, minRetriesPerSec = budgetRetries, percentCanRetry = 0.0)

      // have a policy that allows for way more retries than the budgets allows for
      val policy = RetryPolicy.tries(10, RetryPolicy.WriteExceptionsOnly)

      val filter = new RetryExceptionsFilter[Throwable, Int](policy, Timer.Nil, stats, budget)
      val service: Service[Throwable, Int] = Service.mk(Future.exception)

      val svc = filter.andThen(service)

      Time.withCurrentTimeFrozen { _ =>
        intercept[FailedFastException] {
          Await.result(svc(new FailedFastException("yep")), 5.seconds)
        }

        assert(1 == stats.counter("retries", "budget_exhausted")())
        assert(budgetRetries == stats.stat("retries")().head)
      }
    }

    describe("with RetryPolicy.tries") {

      def runWithTries(retryExceptionsOnly: Boolean): Unit = {
        it("always try once") {
          new TriesFixture(retryExceptionsOnly) {
            when(service(123)) thenReturn Future(321)
            assert(Await.result(retryingService(123), 5.seconds) == 321)
            verify(service)(123)
          }
        }

        it("when failing with WriteExceptions, retry n-1 times") {
          new TriesFixture(retryExceptionsOnly) {
            when(service(123)) thenReturn Future.exception(WriteException(new Exception))
            val f = retryingService(123)
            intercept[WriteException] {
              Await.result(f, 5.seconds)
            }
            verify(service, times(3))(123)
          }
        }

        it("when failed with a non-WriteException, fail immediately") {
          new TriesFixture(retryExceptionsOnly) {
            when(service(123)) thenReturn Future.exception(new Exception("WTF!"))
            val e = intercept[Exception] {
              Await.result(retryingService(123), 5.seconds)
            }
            assert(e.getMessage == "WTF!")
            verify(service)(123)
            assert(retriesStat == Seq(0))
          }
        }

        it("when no retry occurs, no stat update") {
          new TriesFixture(retryExceptionsOnly) {
            when(service(123)) thenReturn Future(goodResponse)
            assert(Await.result(retryingService(123), 5.seconds) == goodResponse)
            assert(retriesStat == Seq(0))
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
            assert(!res.isDefined)
            assert(replyPromise.interrupted == None)

            val exc = new Exception
            res.raise(exc)
            assert(!res.isDefined)
            assert(replyPromise.interrupted == Some(exc))
          }
        }
      }

      describe("using RetryPolicy[(Req, Try[Rep])]") {
        runWithTries(retryExceptionsOnly = false)

        it("when failing with WriteExceptions and non-idempotent request, don't retry") {
          new TriesFixture(retryExceptionsOnly = false) {
            when(service(nonIdempotentRequest)) thenReturn Future.exception(
              WriteException(new Exception)
            )
            val f = retryingService(nonIdempotentRequest)
            intercept[WriteException] {
              Await.result(f, 5.seconds)
            }
            verify(service, times(1))(nonIdempotentRequest)
          }
        }

        it("when succeeds with bad response and idempotent request, retry n-1 times") {
          new TriesFixture(retryExceptionsOnly = false) {
            when(service(idempotentRequest)) thenReturn Future(badResponse)
            val f = retryingService(idempotentRequest)
            Await.result(f, 5.seconds)
            verify(service, times(3))(idempotentRequest)
          }
        }

        it("when succeeds with bad response and non-idempotent request, don't retry") {
          new TriesFixture(retryExceptionsOnly = false) {
            when(service(nonIdempotentRequest)) thenReturn Future(badResponse)
            val f = retryingService(nonIdempotentRequest)
            Await.result(f, 5.seconds)
            verify(service, times(1))(nonIdempotentRequest)
          }
        }
      }

      describe("using RetryPolicy[Try[Nothing]]") {
        runWithTries(retryExceptionsOnly = true)
      }
    }

    describe("with RetryPolicy.backoff: Exception cases") {
      describe("using RetryPolicy[(Req, Try[Rep])]") {
        testExceptionPolicy(
          RetryPolicy.backoff(backoffs)(shouldRetryResponse),
          retryExceptionsOnly = false
        )
      }
      describe("using RetryPolicy[Try[Nothing]]") {
        testExceptionPolicy(
          RetryPolicy.backoff(backoffs)(shouldRetryException),
          retryExceptionsOnly = true
        )
      }
    }

    describe("with RetryPolicy.backoffJava: Exception cases") {
      describe("using RetryPolicy[(Req, Try[Rep])]") {
        testExceptionPolicy(
          RetryPolicy.backoffJava(Backoff.toJava(backoffs), shouldRetryResponse),
          retryExceptionsOnly = false
        )
      }
      describe("using RetryPolicy[Try[Nothing]]") {
        testExceptionPolicy(
          RetryPolicy.backoffJava(Backoff.toJava(backoffs), shouldRetryException),
          retryExceptionsOnly = true
        )
      }
    }

    describe("with Success RetryPolicy.backoff") {
      testSuccessPolicy(RetryPolicy.backoff(backoffs)(shouldRetryResponse))
    }

    describe("with Success RetryPolicy.backoffJava") {
      testSuccessPolicy(RetryPolicy.backoffJava(Backoff.toJava(backoffs), shouldRetryResponse))
    }

    describe("non retryable failures") {
      it("when failed with a Non-Retryable failure, fail immediately") {
        val timer = new MockTimer()
        val stats = new InMemoryStatsReceiver()
        val svc = mock[Service[Int, Int]]
        when(svc.apply(1)).thenReturn(
          Future.exception(Failure.rejected("nack")),
          Future.exception(Failure("not retryable", FailureFlags.NonRetryable)),
          Future.exception(new RuntimeException("never gonna be seen"))
        )

        val policy = RetryPolicy.tries[Try[Nothing]](
          3,
          {
            case Throw(_) => true
          })
        val retryFilter =
          new RetryExceptionsFilter[Int, Int](policy, timer, stats, RetryBudget.Infinite)
        val retryingSvc = retryFilter.andThen(svc)

        Time.withCurrentTimeFrozen { tc =>
          // this req will fail, and enqueue a 2nd req waiting for the timer to advance
          val f = retryingSvc(1)
          verify(svc, times(1))(1)
          assert(!f.isDefined)
          assert(timer.tasks.size == 1)

          // roll the timer and kick off the 2nd request, which should not get
          // retried, despite our policy
          tc.advance(1.second); timer.tick()
          verify(svc, times(2))(1)
          val ex = intercept[Failure] {
            Await.result(f, 5.seconds)
          }
          assert(ex.getMessage == "not retryable")
          assert(stats.stat("retries")() == Seq(1))
        }
      }
    }

    def testExceptionPolicy(policy: RetryPolicy[_], retryExceptionsOnly: Boolean): Unit = {

      it("always try once") {
        new PolicyFixture(policy, retryExceptionsOnly, timer) {
          when(service(123)) thenReturn Future(321)
          assert(Await.result(retryingService(123), 5.seconds) == 321)
          verify(service)(123)
          assert(retriesStat == Seq(0))
        }
      }

      it("when failed with a WriteException, consult the retry strategy") {
        val timer = new MockTimer()
        new PolicyFixture(policy, retryExceptionsOnly, timer) {
          Time.withCurrentTimeFrozen { tc =>
            when(service(123)) thenReturn Future.exception(WriteException(new Exception))
            val f = retryingService(123)
            verify(service)(123)
            assert(!f.isDefined)
            assert(timer.tasks.size == 1)

            when(service(123)) thenReturn Future(321) // we succeed next time; tick!
            tc.advance(1.second); timer.tick()

            verify(service, times(2))(123)
            assert(retriesStat == Seq(1))
            assert(Await.result(f, 5.seconds) == 321)
          }
        }
      }

      it("give up when the retry strategy is exhausted") {
        val timer = new MockTimer()
        new PolicyFixture(policy, retryExceptionsOnly, timer) {
          Time.withCurrentTimeFrozen { tc =>
            when(service(123)) thenReturn Future.exception(
              WriteException(new Exception("i'm exhausted"))
            )
            val f = retryingService(123)
            1 to 3 foreach { i =>
              assert(!f.isDefined)
              verify(service, times(i))(123)
              assert(retriesStat == Seq.empty)
              tc.advance(i.seconds); timer.tick()
            }

            assert(retriesStat == Seq(3))
            assert(f.isDefined)
            assert(Await.ready(f, 5.seconds).poll.get.isThrow)
            val e = intercept[WriteException] {
              Await.result(f, 5.seconds)
            }
            assert(e.getMessage.contains("i'm exhausted"))
          }
        }
      }

      it("when failed with a non-WriteException, fail immediately") {
        val timer = new MockTimer()
        new PolicyFixture(policy, retryExceptionsOnly, timer) {
          when(service(123)) thenReturn Future.exception(new Exception("WTF!"))
          val e = intercept[Exception] {
            Await.result(retryingService(123), 5.seconds)
          }
          assert(e.getMessage == "WTF!")
          verify(service)(123)
          assert(timer.tasks.isEmpty)
          assert(retriesStat == Seq(0))
        }
      }

      it("when failed with a Non-Retryable failure, fail immediately") {
        val timer = new MockTimer()
        new PolicyFixture(policy, retryExceptionsOnly, timer) {
          when(service(123)) thenReturn Future.exception(Failure("WTF!", FailureFlags.NonRetryable))
          val e = intercept[Exception] {
            Await.result(retryingService(123), 5.seconds)
          }
          assert(e.getMessage == "WTF!")
          verify(service)(123)
          assert(timer.tasks.isEmpty)
          assert(retriesStat == Seq(0))
        }
      }

      it("when no retry occurs, no stat update") {
        new PolicyFixture(policy, retryExceptionsOnly, timer) {
          when(service(123)) thenReturn Future(321)
          assert(Await.result(retryingService(123), 5.seconds) == 321)
          assert(retriesStat == Seq(0))
        }
      }

      it("propagate cancellation") {
        new PolicyFixture(policy, retryExceptionsOnly, timer) {
          val replyPromise = new Promise[Int] {
            @volatile var interrupted: Option[Throwable] = None
            setInterruptHandler { case exc => interrupted = Some(exc) }
          }
          when(service(123)) thenReturn replyPromise

          val res = retryingService(123)
          assert(!res.isDefined)
          assert(replyPromise.interrupted == None)

          val exc = new Exception
          res.raise(exc)
          assert(!res.isDefined)
          assert(replyPromise.interrupted == Some(exc))
        }
      }
    }

    def testSuccessPolicy(policy: RetryPolicy[(Int, Try[Int])]): Unit = {

      it("when it succeeds with a bad response, consult the retry strategy") {
        val timer = new MockTimer()
        new PolicyFixture(policy, retryExceptionsOnly = false, timer) {
          Time.withCurrentTimeFrozen { tc =>
            when(service(123)) thenReturn Future(badResponse)
            val f = retryingService(123)
            verify(service)(123)
            assert(!f.isDefined)
            assert(timer.tasks.size == 1)

            when(service(123)) thenReturn Future(goodResponse) // we succeed next time; tick!
            tc.advance(1.second); timer.tick()

            verify(service, times(2))(123)
            assert(retriesStat == Seq(1))
            assert(Await.result(f, 5.seconds) == goodResponse)
          }
        }
      }

      it("return result when the retry strategy is exhausted") {
        val timer = new MockTimer()
        new PolicyFixture(policy, retryExceptionsOnly = false, timer) {
          Time.withCurrentTimeFrozen { tc =>
            when(service(123)) thenReturn Future(badResponse)
            val f = retryingService(123)
            1 to 3 foreach { i =>
              assert(!f.isDefined)
              verify(service, times(i))(123)
              assert(retriesStat == Seq.empty)
              tc.advance(i.seconds); timer.tick()
            }

            assert(retriesStat == Seq(3))
            assert(f.isDefined)
            assert(Await.result(f, 5.seconds) == badResponse)
          }
        }
      }

      it("when it succeeds, return the result immediately") {
        val timer = new MockTimer()
        new PolicyFixture(policy, retryExceptionsOnly = false, timer) {
          when(service(123)) thenReturn Future(goodResponse)
          val f = retryingService(123)
          verify(service)(123)
          assert(timer.tasks.isEmpty)
          assert(retriesStat == Seq(0))
        }
      }

      it("when no retry occurs, no stat update") {
        new PolicyFixture(policy, retryExceptionsOnly = false, timer) {
          when(service(123)) thenReturn Future(321)
          assert(Await.result(retryingService(123), 5.seconds) == 321)
          assert(retriesStat == Seq(0))
        }
      }
    }
  }
}
