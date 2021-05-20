package com.twitter.finagle.exp

import com.netflix.concurrency.limits.limit.AbstractLimit
import com.netflix.concurrency.limits.limit.VegasLimit
import com.netflix.concurrency.limits.Limit
import com.twitter.finagle.{Failure, Service, ServiceFactory, Stack, param}
import com.twitter.util._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.exp.ConcurrencyLimitFilter.ConcurrencyLimitFilter
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.service.FailedService
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.funsuite.AnyFunSuite

class ConcurrencyLimitFilterTest extends AnyFunSuite {

  class Ctx() {
    // NB: [[filterDuration]], [[mockRequestValue]] values are arbitrary
    val statsReceiver: InMemoryStatsReceiver = new InMemoryStatsReceiver()
    val FilterDuration = 10.millis
    val mockRequestValue = 1

    val DefaultInitialLimit: Int = 10
    val DefaultMaxLimit: Int = 50
    val DefaultAlpha: Int = 3
    val DefaultBeta: Int = 6
    val DefaultSmoothing: Double = 1.0

    final def svc(filter: ConcurrencyLimitFilter[Int, Int], p: Promise[Int]): Service[Int, Int] =
      filter.andThen(Service.mk[Int, Int](_ => p))

    def promisesGenerator(size: Int): List[Promise[Int]] = List.fill(size)(new Promise[Int])

    val vegasLimit = VegasLimit
      .newBuilder()
      .alpha(DefaultAlpha)
      .beta(DefaultBeta)
      .smoothing(DefaultSmoothing)
      .initialLimit(DefaultInitialLimit)
      .maxConcurrency(DefaultMaxLimit)
      .build()

    val vegasLimitFilter: ConcurrencyLimitFilter[Int, Int] = new ConcurrencyLimitFilter[Int, Int](
      vegasLimit,
      Stopwatch.timeNanos,
      statsReceiver
    )

    class MockLimitBuilder(init: Int) extends AbstractLimit(init) {
      // Estimated concurrency limit based on mock algorithm
      private val estimatedLimit = new AtomicInteger(init)

      override protected def _update(
        startTime: Long,
        rtt: Long,
        inFlight: Int,
        didDrop: Boolean
      ): Int = {
        // Decrease limit if request did drop. Else increase limit.
        if (didDrop) {
          estimatedLimit.addAndGet(-2)
        } else {
          estimatedLimit.incrementAndGet()
        }
        Math.max(0, estimatedLimit.get)
      }
    }

    // Concurrency Limit Filter (CLF) wrapper
    def clf(limit: Limit): ConcurrencyLimitFilter[Int, Int] =
      new ConcurrencyLimitFilter[Int, Int](
        limit,
        Stopwatch.timeNanos,
        statsReceiver
      )
  }

  test("Filter can be disabled by configuration param") {
    val concurrencySvc =
      ServiceFactory.const[Int, Int](new FailedService(Failure.rejected("goaway")))
    val stats = new InMemoryStatsReceiver

    val s: Stack[ServiceFactory[Int, Int]] =
      ConcurrencyLimitFilter
        .module[Int, Int]
        .toStack(Stack.leaf(Stack.Role("svc"), concurrencySvc))

    val ps: Stack.Params = Stack.Params.empty + param.Stats(stats)
    val sf = s.make(ps + ConcurrencyLimitFilter.Disabled)

    // Make sure the concurrency limit filter module is skipped
    assert(sf.equals(concurrencySvc))
  }

  test("Successful requests should increase limit") {
    val ctx = new Ctx
    import ctx._

    val promisesList = promisesGenerator(DefaultInitialLimit)

    Time.withCurrentTimeFrozen { ctl =>
      val start = Time.now
      ctl.set(start)

      promisesList.foreach(e => svc(vegasLimitFilter, e)(mockRequestValue))

      val startingLimit = vegasLimit.getLimit

      promisesList.foreach(e => {
        ctl.set(start.plus(FilterDuration))
        // all promises succeed
        e.setValue(mockRequestValue)
      })
      val currentLimit = vegasLimit.getLimit
      assert(currentLimit > startingLimit)
    }
  }

  test("Dropped requests should decrease limit") {
    val ctx = new Ctx
    import ctx._

    val promisesList = promisesGenerator(DefaultInitialLimit)

    Time.withCurrentTimeFrozen { ctl =>
      val start = Time.now
      ctl.set(start)

      promisesList.foreach(e => svc(vegasLimitFilter, e)(mockRequestValue))

      val startingLimit = vegasLimit.getLimit

      promisesList.foreach(e => {
        ctl.set(start.plus(FilterDuration))
        // all promises fail
        e.setException(new IllegalArgumentException("mock dropped request"))
      })
      val currentLimit = vegasLimit.getLimit
      assert(currentLimit < startingLimit)
    }
  }

  test("Requests that exceed limit should be rejected") {
    val ctx = new Ctx
    import ctx._

    val testRps = DefaultInitialLimit + 2
    val promisesList = promisesGenerator(testRps)
    val mockFilter = clf(new MockLimitBuilder(DefaultInitialLimit))

    promisesList.foreach(e => svc(mockFilter, e)(mockRequestValue))
    promisesList.foreach(e => e.setValue(mockRequestValue))

    assert(statsReceiver.counter("dropped_requests")() == (testRps - DefaultInitialLimit))
  }

  test("After limit increases, should not reject requests for exceeding initial limit") {

    /**
     *  Initial limit is [[rpsA]]. After all requests in batch #1 succeed, limit should increase.
     *  Batch #2 has [[rpsB]] requests, where rpsB > rpsA. No requests from batch #2 should be
     *  dropped because rpsB does not exceed the new limit.
     */
    val ctx = new Ctx
    import ctx._

    val rpsA = DefaultInitialLimit
    val rpsB = rpsA + 1
    val mockLimit = new MockLimitBuilder(rpsA)
    val mockFilter = clf(mockLimit)
    val promisesListA = promisesGenerator(rpsA)
    val promisesListB = promisesGenerator(rpsB)

    // 1
    promisesListA.foreach(e => svc(mockFilter, e)(mockRequestValue))
    promisesListA.foreach(e => e.setValue(mockRequestValue))

    val currentLimit = mockLimit.getLimit
    assert(currentLimit > DefaultInitialLimit)
    assert(currentLimit >= rpsB)

    // 2
    promisesListB.foreach(e => svc(mockFilter, e)(mockRequestValue))
    promisesListB.foreach(e => e.setValue(mockRequestValue))

    assert(statsReceiver.counter("dropped_requests")() == 0)
  }
}
