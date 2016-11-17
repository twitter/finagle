package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.{NoOpModule, Params}
import com.twitter.finagle._
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier, Retries, RetryBudget, TimeoutFilter}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class MethodBuilderTest
  extends FunSuite
  with Matchers
  with Eventually
  with IntegrationPatience {

  private[this] val timer = new MockTimer()

  private val neverSvc: Service[Int, Int] =
    Service.mk { _ => Future.never }

  private val totalTimeoutStack: Stack[ServiceFactory[Int, Int]] = {
    val svcFactory = ServiceFactory.const(neverSvc)
    // use a no-op module to verify it will get swapped out
    val totalModule = new NoOpModule[ServiceFactory[Int, Int]](
      TimeoutFilter.totalTimeoutRole, "testing total timeout")
    totalModule.toStack(Stack.Leaf(Stack.Role("test"), svcFactory))
  }

  private val totalTimeoutExn = classOf[GlobalRequestTimeoutException]

  private def totalTimeoutParams(timeout: Duration): Stack.Params = {
    Stack.Params.empty +
      TimeoutFilter.TotalTimeout(timeout) +
      param.Timer(timer) +
      param.Stats(NullStatsReceiver)
  }

  private def assertBeforeAndAfterTimeout(
    result: Future[Int],
    timeout: Duration,
    tc: TimeControl,
    expectedException: Class[_ <: RequestTimeoutException]
  ): Unit = {
    // not yet at the timeout
    tc.advance(timeout - 1.millisecond)
    timer.tick()
    assert(!result.isDefined)

    // advance past the timeout
    tc.advance(200.milliseconds)
    timer.tick()
    assert(result.isDefined)

    try Await.result(result, 1.second) catch {
      case ex: RequestTimeoutException =>
        assert(expectedException == ex.getClass)
        ex.getMessage should include(timeout.toString)
      case t: Throwable => fail(t)
    }
  }

  private case class TestStackClient(
      override val stack: Stack[ServiceFactory[Int, Int]],
      override val params: Params)
    extends StackClient[Int, Int] { self =>

    def withStack(stack: Stack[ServiceFactory[Int, Int]]): StackClient[Int, Int] =
      TestStackClient(stack, self.params)

    def withParams(ps: Stack.Params): StackClient[Int, Int] =
      TestStackClient(self.stack, ps)

    def newClient(dest: Name, label: String): ServiceFactory[Int, Int] =
      stack.make(params)

    def newService(dest: Name, label: String): Service[Int, Int] =
      new FactoryToService(newClient(dest, label))
  }

  private def testTotalTimeout(stack: Stack[ServiceFactory[Int, Int]]): Unit = {
    // this is the default if a method doesn't override
    val params = totalTimeoutParams(4.seconds)
    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("dest_paradise", stackClient)

    val fourSecs = methodBuilder.newService("4_secs")
    val twoSecs = methodBuilder.withTimeout.total(2.seconds).newService("2_secs")
    val sixSecs = methodBuilder.withTimeout.total(6.seconds).newService("6_secs")

    // no method-specific override, then a timeout is only supported
    // if the stack originally has the module
    if (stack.contains(TimeoutFilter.totalTimeoutRole)) {
      Time.withCurrentTimeFrozen { tc =>
        assertBeforeAndAfterTimeout(fourSecs(1), 4.seconds, tc, totalTimeoutExn)
      }
    }

    // using a shorter timeout
    Time.withCurrentTimeFrozen { tc =>
      assertBeforeAndAfterTimeout(twoSecs(1), 2.seconds, tc, totalTimeoutExn)
    }

    // using a longer timeout
    Time.withCurrentTimeFrozen { tc =>
      assertBeforeAndAfterTimeout(sixSecs(1), 6.seconds, tc, totalTimeoutExn)
    }
  }

  test("total timeout with module in stack") {
    testTotalTimeout(totalTimeoutStack)
  }

  test("total timeout with module not in stack") {
    testTotalTimeout(totalTimeoutStack.remove(TimeoutFilter.totalTimeoutRole))
  }

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

  test("retries.forClassifier") {
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

  test("retries.forResponse") {
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

  test("retries.forRequestResponse") {
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

  test("retries do not see the total timeout") {
    val stats = new InMemoryStatsReceiver()
    val params =
      Stack.Params.empty +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)
    val stackClient = TestStackClient(totalTimeoutStack, params)
    val methodBuilder = MethodBuilder.from("retry_it", stackClient)

    val client = methodBuilder
      .withTimeout.total(1.second)
      .withRetry.forResponse {
        case Throw(_: GlobalRequestTimeoutException) => true
      }
      .newService("a_client")

    intercept[GlobalRequestTimeoutException] {
      Await.result(client(1), 5.seconds)
    }
    // while we have a RetryFilter, the underlying service returns `Future.never`
    // and as such, the stats are never updated.
    assert(stats.stat("a_client", "retries")() == Seq.empty)
  }

}
