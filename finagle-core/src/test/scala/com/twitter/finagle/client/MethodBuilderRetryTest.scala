package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.client.MethodBuilderTest.TestStackClient
import com.twitter.finagle.service.{ReqRep, ResponseClass, _}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle.{Failure, Service, ServiceFactory, Stack, param}
import com.twitter.util.{Await, Future, Throw}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MethodBuilderRetryTest extends FunSuite {

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

  private def retryMethodBuilder(
    svc: Service[Int, Int],
    stats: StatsReceiver
  ): MethodBuilder[Int, Int] = {
    val svcFactory = ServiceFactory.const(svc)
    val stack = Stack.Leaf(Stack.Role("test"), svcFactory)
    val params =
      Stack.Params.empty +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)
    val stackClient = TestStackClient(stack, params)
    MethodBuilder.from("retry_it", stackClient)
  }

  test("forClassifier") {
    val stats = new InMemoryStatsReceiver()
    val retrySvc = new RetrySvc()
    val methodBuilder = retryMethodBuilder(retrySvc.svc, stats)
    val classifier: ResponseClassifier = {
      case ReqRep(_, Throw(_: IllegalArgumentException)) =>
        ResponseClass.RetryableFailure
    }
    val client = methodBuilder
      .withRetry.forClassifier(classifier)
      .newService("client")

    // the client will retry once
    intercept[NullPointerException] {
      Await.result(client(1), 5.seconds)
    }
    assert(stats.stat("client", "retries")() == Seq(1))
  }

  test("forResponse") {
    val stats = new InMemoryStatsReceiver()
    val retrySvc = new RetrySvc()
    val methodBuilder = retryMethodBuilder(retrySvc.svc, stats)
    val defaults = methodBuilder.newService("defaults")

    val unoOrDos = methodBuilder.withRetry.forResponse {
      case Throw(e) if Seq("uno", "dos").contains(e.getMessage) => true
    }.newService("uno_or_dos")

    val unoOnly = methodBuilder.withRetry.forResponse {
      case Throw(e) if "uno" == e.getMessage => true
    }.newService("uno_only")

    // the client will not retry anything, let alone have a retry filter,
    // and see the 1st response
    intercept[IllegalArgumentException] {
      Await.result(defaults(1), 5.seconds)
    }
    assert(stats.stat("defaults", "retries")() == Seq.empty)
    retrySvc.reqNum = 0

    // this client will retry the first 2 responses and then see the 3rd response
    assert(3 == Await.result(unoOrDos(1), 5.seconds))
    assert(stats.stat("uno_or_dos", "retries")() == Seq(2))
    retrySvc.reqNum = 0

    // this client will retry the first response and then see the 2nd response
    intercept[NullPointerException] {
      Await.result(unoOnly(1), 5.seconds)
    }
    assert(stats.stat("uno_only", "retries")() == Seq(1))
    retrySvc.reqNum = 0
  }

  test("forRequestResponse") {
    val stats = new InMemoryStatsReceiver()
    val retrySvc = new RetrySvc()
    val methodBuilder = retryMethodBuilder(retrySvc.svc, stats)

    val reqIsOne = methodBuilder.withRetry.forRequestResponse {
      case (req, _) if req == 1 => true
    }.newService("req_is_one")

    val reqIsOneOrRepIsUno = methodBuilder.withRetry.forRequestResponse {
      case (req, _) if req == 1 => true
      case (_, Throw(t)) if "uno" == t.getMessage => true
    }.newService("req_is_one_or_rep_is_uno")

    // this will keep retrying until we hit the max retries allowed
    assert(2 == MethodBuilderRetry.MaxRetries)
    assert(3 == Await.result(reqIsOne(1), 5.seconds))
    assert(stats.stat("req_is_one", "retries")() == Seq(2))
    retrySvc.reqNum = 0

    // should not retry this, as the request is not 1
    intercept[IllegalArgumentException] {
      Await.result(reqIsOne(2), 5.seconds)
    }
    assert(stats.stat("req_is_one", "retries")() == Seq(2, 0))
    retrySvc.reqNum = 0

    // retry once, since the rep will be uno
    intercept[NullPointerException] {
      Await.result(reqIsOneOrRepIsUno(2), 5.seconds)
    }
    assert(stats.stat("req_is_one_or_rep_is_uno", "retries")() == Seq(1))
    retrySvc.reqNum = 0
  }

  test("retries do not apply to failures handled by the RequeueFilter") {
    val stats = new InMemoryStatsReceiver()
    val svc = Service.mk[Int, Int] { _ =>
      Future.exception(Failure.rejected("nuh uh"))
    }
    val methodBuilder = retryMethodBuilder(svc, stats)
    val client = methodBuilder.withRetry.forResponse {
      case Throw(f: Failure) if f.isFlagged(Failure.Restartable) => true
    }.newService("client")

    val ex = intercept[Failure] {
      Await.result(client(1), 5.seconds)
    }
    assert(ex.isFlagged(Failure.Restartable))
    assert(stats.stat("client", "retries")() == Seq(0))
  }
}
