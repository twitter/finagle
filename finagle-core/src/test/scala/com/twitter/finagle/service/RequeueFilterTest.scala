package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{FailedFastException, Service}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RequeueFilterTest extends FunSuite {

  test("respects maxRetriesPerReq") {
    val stats = new InMemoryStatsReceiver()

    val minRetries = 10
    val percentRequeues = 0.5
    val filter = new RequeueFilter[Throwable, Int](
      RetryBudget(1.second, minRetries, 0.0, Stopwatch.timeMillis),
      Backoff.constant(Duration.Zero),
      stats,
      () => true,
      percentRequeues,
      DefaultTimer.twitter)

    val svc = filter.andThen(Service.mk(Future.exception))

    intercept[FailedFastException] {
      Await.result(svc(new FailedFastException("lolll")), 5.seconds)
    }

    assert(minRetries * percentRequeues == stats.counter("requeues")())
    assert(Seq(minRetries * percentRequeues) == stats.stat("requeues_per_request")())
    // the budget is not considered exhausted if we only used
    // our maxRetriesPerReq
    assert(0 == stats.counter("budget_exhausted")())
  }

  test("exhausts budget") {
    // this is a bit odd to test without multiple clients,
    // we force it by allowing 200% of the budget to be used
    val stats = new InMemoryStatsReceiver()

    val minRetries = 10
    val percentRequeues = 2.0
    val filter = new RequeueFilter[Throwable, Int](
      RetryBudget(1.second, minRetries, 0.0, Stopwatch.timeMillis),
      Backoff.constant(Duration.Zero),
      stats,
      () => true,
      percentRequeues,
      DefaultTimer.twitter)

    val svc = filter.andThen(Service.mk(Future.exception))

    intercept[FailedFastException] {
      Await.result(svc(new FailedFastException("lolll")), 5.seconds)
    }

    assert(minRetries == stats.counter("requeues")())
    assert(Seq(minRetries) == stats.stat("requeues_per_request")())
    assert(1 == stats.counter("budget_exhausted")())
  }

  test("tracks requeues_per_request") {
    val stats = new InMemoryStatsReceiver()
    def perReqRequeues: Seq[Float] =
      stats.stat("requeues_per_request")()

    val minRetries = 10
    val percentRequeues = 0.5 // allow only 50% to be used
    val retryBudget = RetryBudget(1.second, minRetries, 0.0, Stopwatch.timeMillis)
    val filter = new RequeueFilter[String, Int](
      retryBudget,
      Backoff.constant(Duration.Zero),
      stats,
      () => true,
      percentRequeues,
      DefaultTimer.twitter)

    var numNos = 0
    val svc = filter.andThen(Service.mk { s: String =>
      if (s == "no" && numNos == 0) {
        numNos += 1
        Future.exception(new FailedFastException(s))
      } else if (s == "fail") {
        Future.exception(new FailedFastException(s))
      } else {
        Future.value(s.length)
      }
    })

    // a successful request
    Await.ready(svc("hi"), 5.seconds)
    assert(Seq(0) == perReqRequeues)

    // a request that fails once
    Await.ready(svc("no"), 5.seconds)
    assert(Seq(0, 1) == perReqRequeues)

    // a request that fails until it runs out of attempts
    assert(0 == stats.counter("request_limit")()) // verify a precondition
    Await.ready(svc("fail"), 5.seconds)
    // the 1 failed request knocks our balance from 10 to 9,
    // then we get 50% of that, which is 5.
    assert(Seq(0, 1, 5) == perReqRequeues)
    assert(1 == stats.counter("request_limit")())
  }

  test("applies delay") {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer()
      val stats = new InMemoryStatsReceiver()
      val minDelay = 1.second
      val scheduleLength = 3
      val schedule = Backoff.exponential(minDelay, 2).take(scheduleLength)

      val filter = new RequeueFilter[Throwable, Int](
        RetryBudget(1.second, 10, 0.0, Stopwatch.timeMillis),
        schedule,
        stats,
        () => true,
        1.0,
        timer)

      val svc = filter.andThen(Service.mk(Future.exception))
      val response = svc(new FailedFastException("12345"))

      var expectedDelay = minDelay

      1.to(scheduleLength).foreach { attempt =>
        assert(!response.isDefined)

        // trigger the next retry by advancing past the delay
        timeControl.advance(expectedDelay)
        timer.tick()

        // using exponential policy, keep up.
        expectedDelay *= 2

        assert(attempt == stats.counter("requeues")())
      }

      // at this point we should have exhausted our budget
      assert(1 == stats.counter("budget_exhausted")())
      intercept[FailedFastException] {
        Await.result(response, 1.second)
      }
    }
  }

  test("uses response classifier") {
    val timer = new MockTimer
    val stats = new InMemoryStatsReceiver
    val filter = new RequeueFilter[String, String](
      RetryBudget(1.second, 1, 0.0, Stopwatch.timeMillis),
      Backoff.const(Duration.Zero),
      stats,
      () => true,
      1.0,
      timer,
      ResponseClassifier.named("TestClassifier") {
        case ReqRep(_, Return("fail")) => ResponseClass.NonRetryableFailure
        case ReqRep(_, Return("retry")) => ResponseClass.RetryableFailure
      }
    )
    val svc = filter.andThen(Service.mk[String, String] {
      case "ok" => Future.value("ok")
      case "fail" => Future.value("fail")
      case "retry" => Future.value("retry")
      case "failfast" => Future.exception(new FailedFastException("failfast"))
      case in => Future.exception(new IllegalArgumentException(in))
    })

    val requeues = stats.counter("requeues")

    Time.withCurrentTimeFrozen { clock =>
      // successes are successes
      assert(Await.result(svc("ok")) == "ok")
      assert(0 == requeues())

      // failures are failures
      assert(Await.result(svc("fail")) == "fail")
      assert(0 == requeues())
      clock.advance(1.second)
      timer.tick()

      // RetryableFailures are retried
      assert(Await.result(svc("retry")) == "retry")
      assert(1 == requeues())
      clock.advance(1.second)
      timer.tick()

      // RetryableWriteExceptions are retried
      assert(Await.result(svc("failfast").liftToTry).isThrow)
      assert(2 == requeues())
      clock.advance(1.second)
      timer.tick()

      // other failures are not retried
      assert(Await.result(svc("illegal").liftToTry).isThrow)
      assert(2 == requeues())
    }
  }
}
