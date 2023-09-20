package com.twitter.finagle.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.client.MethodBuilderTest.TestStackClient
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle._
import com.twitter.util.{Await, Future, Throw}
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.AtomicInteger

class MethodBuilderRetryTest extends AnyFunSuite {

  private[this] class RetrySvc {
    var reqNum = 0
    val svc: Service[Int, Int] = Service.mk { _ =>
      reqNum += 1
      if (reqNum == 1) {
        Future.exception(new IllegalArgumentException("uno"))
      } else if (reqNum == 2) {
        Future.exception(new NullPointerException("dos"))
      } else {
        Future.value(reqNum)
      }
    }
  }

  private val clientName = "some_client"

  private def retryMethodBuilder(
    svc: Service[Int, Int],
    stats: StatsReceiver,
    params: Stack.Params = Stack.Params.empty
  ): MethodBuilder[Int, Int] = {
    val svcFactory = ServiceFactory.const(svc)
    val stack = Stack.leaf(Stack.Role("test"), svcFactory)
    val ps =
      Stack.Params.empty +
        param.Label(clientName) +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite) ++
        params
    val stackClient = TestStackClient(stack, ps)
    MethodBuilder.from("retry_it", stackClient)
  }

  test("uses stack's ResponseClassifier by default") {
    val stats = new InMemoryStatsReceiver()
    val retrySvc = new RetrySvc()
    val retryIllegalArgClassifier: ResponseClassifier = {
      case ReqRep(_, Throw(_: IllegalArgumentException)) =>
        ResponseClass.RetryableFailure
    }
    val methodBuilder = retryMethodBuilder(
      retrySvc.svc,
      stats,
      Stack.Params.empty + param.ResponseClassifier(retryIllegalArgClassifier)
    )
    val defaults = methodBuilder.newService("defaults")

    // the client will use the stack's ResponseClassifier, which
    // will retry the 1st response of an IllegalArgumentException
    intercept[NullPointerException] {
      Await.result(defaults(1), 5.seconds)
    }
    assert(stats.stat(clientName, "defaults", "retries")() == Seq(1))
  }

  test("retries can be disabled") {
    val stats = new InMemoryStatsReceiver()
    val retrySvc = new RetrySvc()
    val methodBuilder = retryMethodBuilder(retrySvc.svc, stats)
    val noRetries = methodBuilder.withRetry.disabled
      .newService("no_retries")

    // the client will not retry anything, let alone have a retry filter,
    // and see the 1st response
    intercept[IllegalArgumentException] {
      Await.result(noRetries(1), 5.seconds)
    }
    assert(stats.stat(clientName, "no_retries", "retries")() == Seq.empty)
  }

  test("forClassifier") {
    val stats = new InMemoryStatsReceiver()
    val retrySvc = new RetrySvc()
    val methodBuilder = retryMethodBuilder(retrySvc.svc, stats)
    val classifier: ResponseClassifier = {
      case ReqRep(_, Throw(_: IllegalArgumentException)) =>
        ResponseClass.RetryableFailure
    }
    val client = methodBuilder.withRetry
      .forClassifier(classifier)
      .newService("client")

    // the client will retry once
    intercept[NullPointerException] {
      Await.result(client(1), 5.seconds)
    }
    assert(stats.stat(clientName, "client", "retries")() == Seq(1))
  }

  test("maxRetries") {
    val stats = new InMemoryStatsReceiver()
    class InfiniteRetrySvc {
      var reqNum = 0
      val svc: Service[Int, Int] = Service.mk { _ =>
        reqNum += 1
        Future.exception(new IllegalArgumentException("uno"))
      }
    }
    val retrySvc = new InfiniteRetrySvc
    val methodBuilder = retryMethodBuilder(retrySvc.svc, stats)
    val classifier: ResponseClassifier = {
      case ReqRep(_, Throw(_: IllegalArgumentException)) =>
        ResponseClass.RetryableFailure
    }
    val client = methodBuilder.withRetry
      .forClassifier(classifier)
      .withRetry.maxRetries(5)
      .newService("client")

    // the client will retry 5 times and then fail
    intercept[IllegalArgumentException] {
      Await.result(client(1), 5.seconds)
    }
    assert(stats.stat(clientName, "client", "retries")() == Seq(5))
  }

  test("scoped to clientName if methodName is None") {
    val stats = new InMemoryStatsReceiver()
    val retrySvc = new RetrySvc()
    val methodBuilder = retryMethodBuilder(retrySvc.svc, stats)
    val classifier: ResponseClassifier = {
      case ReqRep(_, Throw(_: IllegalArgumentException)) =>
        ResponseClass.RetryableFailure
    }
    val client = methodBuilder.withRetry
      .forClassifier(classifier)
      .newService

    // the client will retry once
    intercept[NullPointerException] {
      Await.result(client(1), 5.seconds)
    }
    assert(stats.stat(clientName, "retries")() == Seq(1))
  }

  test("retries do not apply to failures handled by the RequeueFilter") {
    val stats = new InMemoryStatsReceiver()
    val svc = Service.mk[Int, Int] { _ => Future.exception(Failure.rejected("nuh uh")) }
    val methodBuilder = retryMethodBuilder(svc, stats)
    val client = methodBuilder.withRetry
      .forClassifier {
        case ReqRep(_, Throw(f: Failure)) if f.isFlagged(FailureFlags.Retryable) =>
          ResponseClass.RetryableFailure
      }
      .newService("client")

    val ex = intercept[Failure] {
      Await.result(client(1), 5.seconds)
    }
    assert(ex.isFlagged(FailureFlags.Retryable))
    assert(stats.stat(clientName, "client", "retries")() == Seq(0))
  }

  test("retries and requeues do not double deposit RetryBudget") {
    val stats = new InMemoryStatsReceiver()
    val budget = Retries.Budget(RetryBudget(ttl = 10.seconds, 10, 0.1))

    val svc = Service.mk[Int, Int] { _ =>
        Future.value(1)
    }

    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val ps =
      Stack.Params.empty +
        param.Label(clientName) +
        param.Stats(stats) +
        budget
    val stackClient = TestStackClient(stack, ps)

    val methodBuilder = MethodBuilder.from("retry_it", stackClient)
    val client = methodBuilder
      .newService("client")

    assert(budget.retryBudget.balance == 100)
    (1 to 10).foreach(_ => Await.result(client(1), 5.seconds))
    assert(budget.retryBudget.balance == 101)

    assert(stats.stat(clientName, "client", "retries")() == Seq.fill(10)(0))
  }


  test("MethodBuilder withRetry.disabled should allow RequeueFilter to deposit RetryBudget") {
    val stats = new InMemoryStatsReceiver()
    val budget = Retries.Budget(RetryBudget(ttl = 10.seconds, 10, 0.1))

    val svc = Service.mk[Int, Int] { _ =>
      Future.value(1)
    }

    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val ps =
      Stack.Params.empty +
        param.Label(clientName) +
        param.Stats(stats) +
        budget
    val stackClient = TestStackClient(stack, ps)

    val methodBuilder = MethodBuilder.from("retry_it", stackClient)
    val client = methodBuilder
      .withRetry.disabled
      .newService("client")

    assert(budget.retryBudget.balance == 100)
    (1 to 10).foreach(_ => Await.result(client(1), 5.seconds))
    assert(budget.retryBudget.balance == 101)

    assert(stats.stat(clientName, "client", "retries")() == List()) // does not exist
  }
}
