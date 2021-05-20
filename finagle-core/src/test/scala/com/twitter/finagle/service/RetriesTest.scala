package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle._
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.funsuite.AnyFunSuite

class RetriesTest extends AnyFunSuite {

  private[this] class MyRetryEx(val flags: Long = FailureFlags.Empty)
      extends Exception
      with FailureFlags[MyRetryEx] {
    protected def copyWithFlags(flags: Long): MyRetryEx = new MyRetryEx(flags)
  }
  private[this] class AnotherEx extends Exception

  private[this] val retryFn: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(_: MyRetryEx) => true
  }

  private[this] def newRetryPolicy(retries: Int) =
    RetryPolicy.tries(
      retries + 1, // 1 request and `retries` retries
      retryFn
    )

  private[this] val requeableEx =
    Failure.wrap(new RuntimeException("yep"), FailureFlags.Retryable)

  private[this] val notRequeueableEx = new RuntimeException("nope")

  private val end: Stack[ServiceFactory[Exception, Int]] = Stack.leaf(
    Stack.Role("test"),
    ServiceFactory.const(
      Service.mk[Exception, Int] { req => Future.exception(req) }
    )
  )

  private val minBudget = 3

  private def newBudget(): RetryBudget =
    RetryBudget(
      ttl = 1.second, // simplifies the math such that minRetries == minRetriesPerSecond
      minRetriesPerSec = minBudget,
      percentCanRetry = 0.0, // this shouldn't be a factor because we are relying on the reserve
      nowMillis = Stopwatch.timeMillis
    )

  test(
    "moduleRequeueable retries service acquisition `Retries.Effort` times on retryable failure") {
    val stats = new InMemoryStatsReceiver()

    val params = Stack.Params.empty +
      param.Stats(stats) +
      Retries.Budget(RetryBudget.Empty)

    val failingFactory: Stack[ServiceFactory[String, String]] = Stack.leaf(
      Stack.Role("FailingFactory"),
      new FailingFactory[String, String](new Failure("boom", flags = FailureFlags.Retryable))
    )

    val svcFactory: ServiceFactory[String, String] =
      Retries.moduleRequeueable.toStack(failingFactory).make(params)

    intercept[Exception] {
      Await.result(svcFactory(), 5.seconds)
    }

    assert(stats.counter("retries", "requeues")() == Retries.Effort)
  }

  test(
    "moduleRequeueable retries service acquisition `Retries.Effort` times on retryable failure " +
      "for each service application when using FactoryToService"
  ) {
    val stats = new InMemoryStatsReceiver()

    val params = Stack.Params.empty +
      param.Stats(stats) +
      Retries.Budget(RetryBudget.Empty)

    val failingFactory: Stack[ServiceFactory[String, String]] = Stack.leaf(
      Stack.Role("test"),
      new FailingFactory[String, String](new Failure("boom", flags = FailureFlags.Retryable))
    )

    val svcFactory: ServiceFactory[String, String] =
      Retries.moduleRequeueable.toStack(failingFactory).make(params)

    val svc = new FactoryToService(svcFactory)

    intercept[Exception] {
      Await.result(svc("hello"), 5.seconds)
    }

    assert(stats.counter("retries", "requeues")() == Retries.Effort)

    intercept[Exception] {
      Await.result(svc("hello"), 5.seconds)
    }

    assert(stats.counter("retries", "requeues")() == Retries.Effort * 2)
  }

  test("moduleRetryableWrites only does requeues") {
    val stats = new InMemoryStatsReceiver()
    val budget = newBudget()

    val retryAll: PartialFunction[Try[Nothing], Boolean] = {
      case _ => true
    }

    val params = Stack.Params.empty +
      param.Stats(stats) +
      Retries.Policy(RetryPolicy.tries(10, retryAll)) +
      Retries.Budget(budget)

    val svcFactory: ServiceFactory[Exception, Int] =
      Retries.moduleRequeueable.toStack(end).make(params)

    val svc: Service[Exception, Int] =
      Await.result(svcFactory(), 5.seconds)

    Time.withCurrentTimeFrozen { tc =>
      // each request will use up 1 retry from the budget due to the
      // caps maxRetriesPerReq limits
      1.to(minBudget).foreach { i =>
        val f = intercept[Failure] {
          Await.result(svc(requeableEx), 5.seconds)
        }
        assert(f.getMessage == requeableEx.getMessage)
        assert(stats.counter("retries", "requeues")() == i)
      }
      // and check that we didn't do anything in a retry filter
      assert(!stats.stats.contains(Seq("retries")))

      // next, a request that is not eligible for requeues nor retries
      tc.advance(1.minute) // make sure we have requeue budget from the reserve
      assert(budget.balance == minBudget)
      val f2 = intercept[RuntimeException] {
        Await.result(svc(notRequeueableEx), 5.seconds)
      }
      assert(f2.getMessage == notRequeueableEx.getMessage)
      // no additional requeues, even though we had budget
      assert(stats.counter("retries", "requeues")() == minBudget)

      // and nothing got retried in the RetryFilter
      assert(!stats.stats.contains(Seq("retries")))
    }
  }

  test("moduleWithRetryPolicy requeues without retries") {
    val stats = new InMemoryStatsReceiver()
    val budget = newBudget()

    val params = Stack.Params.empty +
      param.Stats(stats) +
      Retries.Policy(RetryPolicy.Never) + // explicitly turn it off
      Retries.Budget(budget)

    val svcFactory: ServiceFactory[Exception, Int] =
      Retries.moduleWithRetryPolicy.toStack(end).make(params)

    val svc: Service[Exception, Int] =
      Await.result(svcFactory(), 5.seconds)

    Time.withCurrentTimeFrozen { tc =>
      // each request will use up 1 retry from the budget due to the
      // caps maxRetriesPerReq limits
      1.to(minBudget).foreach { i =>
        val f = intercept[Failure] {
          Await.result(svc(requeableEx), 5.seconds)
        }
        assert(f.getMessage == requeableEx.getMessage)
        assert(stats.counter("retries", "requeues")() == i)
      }
      // and check that we didn't do anything in a retry filter
      assert(!stats.stats.contains(Seq("retries")))

      // next, a request that is not eligible for requeues nor retries
      tc.advance(1.minute) // make sure we have requeue budget from the reserve
      assert(budget.balance == minBudget)
      val f2 = intercept[RuntimeException] {
        Await.result(svc(notRequeueableEx), 5.seconds)
      }
      assert(f2.getMessage == notRequeueableEx.getMessage)
      // no additional requeues, even though we had budget
      assert(stats.counter("retries", "requeues")() == minBudget)

      // and nothing got retried in the RetryFilter
      assert(!stats.stats.contains(Seq("retries")))
    }
  }

  test("moduleWithRetryPolicy retries with no requeues") {
    val stats = new InMemoryStatsReceiver()

    val budget =
      RetryBudget(
        ttl = 20.seconds, // give a long window so we don't need to worry
        minRetriesPerSec = 1, // works out to 20 minimum retries per ttl
        percentCanRetry = 0.0, // this shouldn't be a factor because we are relying on the reserve
        nowMillis = Stopwatch.systemMillis
      )

    val params = Stack.Params.empty +
      param.Stats(stats) +
      Retries.Policy(newRetryPolicy(100)) + // way higher than the budget
      Retries.Budget(budget)

    val svcFactory: ServiceFactory[Exception, Int] =
      Retries.moduleWithRetryPolicy.toStack(end).make(params)

    val svc: Service[Exception, Int] =
      Await.result(svcFactory(), 5.seconds)

    intercept[MyRetryEx] {
      Await.result(svc(new MyRetryEx()), 5.seconds)
    }

    // should not be requeued, but should have been retried
    // up to what the budget allows for.
    assert(stats.counters(Seq("retries", "requeues")) == 0)
    // the budget gives us 20, we should use only that many
    // and not all the way up to the RetryPolicy's allotment of 100.
    assert(Seq(20f) == stats.stats(Seq("retries")))
    assert(1 == stats.counter("retries", "budget_exhausted")())
  }

  test("moduleWithRetryPolicy neither requeued nor retried") {
    val stats = new InMemoryStatsReceiver()

    val params = Stack.Params.empty +
      param.Stats(stats) +
      Retries.Policy(newRetryPolicy(10)) + // this count doesn't come into play
      Retries.Budget(newBudget())

    val svcFactory: ServiceFactory[Exception, Int] =
      Retries.moduleWithRetryPolicy.toStack(end).make(params)

    val svc: Service[Exception, Int] =
      Await.result(svcFactory(), 5.seconds)

    intercept[AnotherEx] {
      Await.result(svc(new AnotherEx()), 5.seconds)
    }

    // should not have triggered either requeue or retries
    assert(stats.counters(Seq("retries", "requeues")) == 0)
    assert(Seq(0.0) == stats.stats(Seq("retries")))
    assert(0 == stats.counter("retries", "budget_exhausted")())
  }

  /** Uses 4 retries for the RetryPolicy */
  private def endToEndToEndSvc(
    stats: InMemoryStatsReceiver,
    backReqs: AtomicInteger,
    mkBudget: () => RetryBudget
  ): Service[Exception, Int] = {
    val midParams = Stack.Params.empty +
      param.Stats(stats.scope("mid")) +
      Retries.Budget(mkBudget()) +
      Retries.Policy(newRetryPolicy(retries = 4))

    val frontParams = Stack.Params.empty +
      param.Stats(stats.scope("front")) +
      Retries.Budget(mkBudget()) +
      Retries.Policy(newRetryPolicy(retries = 4))

    val backSvc = ServiceFactory.const(
      Service.mk[Exception, Int] { req =>
        backReqs.incrementAndGet()
        Future.exception(req)
      }
    )

    // wire em together.
    val midToBack: Stack[ServiceFactory[Exception, Int]] =
      Stack.leaf(Stack.Role("mid-back"), backSvc)
    val midSvcFactory = Retries.moduleWithRetryPolicy.toStack(midToBack).make(midParams)

    val frontToMid: Stack[ServiceFactory[Exception, Int]] =
      Stack.leaf(Stack.Role("front-mid"), midSvcFactory)
    val frontSvcFactory = Retries.moduleWithRetryPolicy.toStack(frontToMid).make(frontParams)
    Await.result(frontSvcFactory(), 5.seconds)
  }

  private def nRetries(retriesStat: Seq[Float]): Int = {
    retriesStat.foldLeft(0) { (sum, next) => sum + next.toInt }
  }

  test("moduleWithRetryPolicy end to end to end with RetryBudget") {
    val stats = new InMemoryStatsReceiver()
    val backReqs = new AtomicInteger()
    val retryPercent = 0.2 // 20% retries
    def mkBudget() =
      RetryBudget(
        60.seconds,
        0, // keep minimum out of this to simplify
        retryPercent,
        Stopwatch.timeMillis
      )

    val svc = endToEndToEndSvc(stats, backReqs, mkBudget _)

    val numReqs = 100
    Time.withCurrentTimeFrozen { _ =>
      0.until(numReqs).foreach { _ =>
        intercept[MyRetryEx] {
          Await.result(svc(new MyRetryEx()), 5.seconds)
        }
      }

      // Verify that the NonRetryable flag is respected and only one layer here
      // retries requests. Behavior should be the same as only having one filter.
      assert(0 == nRetries(stats.stats(Seq("front", "retries"))))
      assert((numReqs * 0.2).toInt == nRetries(stats.stats(Seq("mid", "retries"))))
      assert((numReqs * 1.2).toInt == backReqs.get)
    }
  }

  test("moduleWithRetryPolicy end to end to end without RetryBudget") {
    val stats = new InMemoryStatsReceiver()
    val backReqs = new AtomicInteger()
    def mkBudget() = RetryBudget.Infinite

    val svc = endToEndToEndSvc(stats, backReqs, mkBudget _)

    val retries = 4
    val numReqs = 100
    Time.withCurrentTimeFrozen { _ =>
      0.until(numReqs).foreach { i =>
        intercept[MyRetryEx] {
          Await.result(svc(new MyRetryEx()), 5.seconds)
        }
      }

      assert(
        numReqs * retries ==
          nRetries(stats.stats(Seq("front", "retries")))
      )
      assert(
        (numReqs * retries) + (numReqs * retries * retries) ==
          nRetries(stats.stats(Seq("mid", "retries")))
      )
      // there is a 25x multiplier. each initial front attempt triggers
      // 1 attempt + 4 retries = 5 reqs from the mid to the backend,
      // and the front will do that a total of 5 times (so 5 * 5 = 25)
      assert(numReqs * 25 == backReqs.get)
    }
  }

  test("budget gauge lifecycle") {
    val stats = new InMemoryStatsReceiver()
    def budgetGauge: Option[Float] =
      stats.gauges.get(Seq("retries", "budget")).map(_())

    assert(budgetGauge.isEmpty)

    // creating the service factory creates the gauge
    val params = Stack.Params.empty +
      param.Stats(stats) +
      Retries.Budget(RetryBudget.Empty)
    val svcFactory: ServiceFactory[Exception, Int] =
      Retries.moduleRequeueable.toStack(end).make(params)
    assert(budgetGauge.exists(_ == 0))

    // closing a service should not touch it
    val svc: Service[Exception, Int] =
      Await.result(svcFactory(), 5.seconds)
    svc.close(Duration.Zero)
    assert(budgetGauge.exists(_ == 0))

    // closing the factory should remove the gauge
    svcFactory.close(Duration.Zero)
    assert(budgetGauge.isEmpty)
  }

  test("Sets RetryBudget param for lower modules when none is configured") {
    val verifyModule =
      new Stack.Module[ServiceFactory[Unit, Unit]] {
        val role = Stack.Role("verifyRetryBudget")
        val description = "Verify that the retry budget was added to the params"

        val parameters = Seq.empty

        def make(params: Stack.Params, next: Stack[ServiceFactory[Unit, Unit]]) = {
          assert(params.size == 1 && params.head._2.isInstanceOf[Retries.Budget])
          Stack.leaf(this, next.make(params))
        }
      }

    val factory = new StackBuilder[ServiceFactory[Unit, Unit]](nilStack[Unit, Unit])
      .push(verifyModule)
      .push(Retries.moduleRequeueable)
      .make(Stack.Params.empty)

    factory()
  }

  test("Sets same RetryBudget param for lower modules when one is configured") {
    val budget = Retries.Budget(newBudget())

    val verifyModule =
      new Stack.Module[ServiceFactory[Unit, Unit]] {
        val role = Stack.Role("verifyRetryBudget")
        val description = "Verify that the same retry budget was added to the params"

        val parameters = Seq.empty

        def make(params: Stack.Params, next: Stack[ServiceFactory[Unit, Unit]]) = {
          assert(params.size == 1 && (params[Retries.Budget] eq budget))
          Stack.leaf(this, next.make(params))
        }
      }

    val factory = new StackBuilder[ServiceFactory[Unit, Unit]](nilStack[Unit, Unit])
      .push(verifyModule)
      .push(Retries.moduleRequeueable)
      .make(Stack.Params.empty + budget)

    factory()
  }

  test("hashCode and equals for Retries.Budget only checks RetryBudget, not Backoff stream") {
    val budget = Retries.Budget(RetryBudget.Empty, Backoff.const(1.second))
    val budgetWithDifferentBackoffStream =
      Retries.Budget(RetryBudget.Empty, Backoff.const(2.second))
    val differentBudget = Retries.Budget(RetryBudget.Infinite, Backoff.const(3.second))

    assert(budget.equals(budget))
    assert(budget.equals(budgetWithDifferentBackoffStream))
    assert(!budget.equals(differentBudget))

    assert(budget.## == budget.##)
    assert(budget.## == budgetWithDifferentBackoffStream.##)
    assert(budget.## != differentBudget.##)
  }
}
