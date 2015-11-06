package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.{FailedFastException, Service}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Stopwatch, Await, Future}
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
      stats,
      () => true,
      percentRequeues)

    val svc = filter.andThen(Service.mk(Future.exception))

    intercept[FailedFastException] {
      Await.result(svc(new FailedFastException("lolll")), 5.seconds)
    }

    assert(minRetries * percentRequeues == stats.counter("requeues")())
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
      stats,
      () => true,
      percentRequeues)

    val svc = filter.andThen(Service.mk(Future.exception))

    intercept[FailedFastException] {
      Await.result(svc(new FailedFastException("lolll")), 5.seconds)
    }

    assert(minRetries == stats.counter("requeues")())
    assert(1 == stats.counter("budget_exhausted")())
  }

}
