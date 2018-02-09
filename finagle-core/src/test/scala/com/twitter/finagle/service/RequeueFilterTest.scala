package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.util._
import java.io.IOException
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
      DefaultTimer
    )

    val svc = filter.andThen(Service.mk(Future.exception))

    val exn = intercept[FailedFastException] {
      Await.result(svc(new FailedFastException("lolll")), 5.seconds)
    }

    assert(exn.isFlagged(FailureFlags.NonRetryable))
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
      DefaultTimer
    )

    val svc = filter.andThen(Service.mk(Future.exception))

    val exn = intercept[FailedFastException] {
      Await.result(svc(new FailedFastException("lolll")), 5.seconds)
    }

    assert(exn.isFlagged(FailureFlags.NonRetryable))
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
      DefaultTimer
    )

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
        timer
      )

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

  test("retries propagated in context") {
    val minRetries = 10
    val percentRequeues = 0.5
    val filter = new RequeueFilter[Throwable, Int](
      RetryBudget(1.second, minRetries, 0.0, Stopwatch.timeMillis),
      Backoff.constant(Duration.Zero),
      NullStatsReceiver,
      () => true,
      percentRequeues,
      DefaultTimer
    )

    val stats = new InMemoryStatsReceiver()

    val retriesStat = stats.stat("retry_context_retries")

    val svcFactory = ServiceFactory.const(
      filter.andThen(Service.mk[Throwable, Int] { req =>
        context.Retries.current.foreach { retries =>
          retriesStat.add(retries.attempt)
        }
        Future.exception(req)
      })
    )

    val svc = Await.result(svcFactory(), 5.seconds)

    Time.withCurrentTimeFrozen { _ =>
      intercept[FailedFastException] {
        Await.result(svc(new FailedFastException("bad")), 5.seconds)
      }

      // We should have retried 5 times
      val retriesInContext = List(0, 1, 2, 3, 4, 5)

      assert(stats.stat("retry_context_retries")().map(_.toInt) == retriesInContext)
    }
  }

  test("Requeueable.unapply for retryable exceptions") {
    Seq(
      Failure.rejected("rejected"),
      WriteException(new RuntimeException())
    ).foreach {
      case RequeueFilter.Requeueable(_) =>
      case x => fail(s"should be Requeueable: $x")
    }
  }

  test("Requeueable.unapply for non-retryable exceptions") {
    Seq(
      Failure("not retryable", Failure.NonRetryable),
      Failure("interrupted", Failure.Interrupted),
      new IOException("an io exception")
    ).foreach {
      case RequeueFilter.Requeueable(x) => fail(s"should not be Requeueable: $x")
      case _ =>
    }
  }

}
