package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.{NoOpModule, Params}
import com.twitter.finagle._
import com.twitter.finagle.service.{ReqRep, ResponseClass, Retries, RetryBudget, TimeoutFilter}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

private object MethodBuilderTest {
  private val neverSvc: Service[Int, Int] =
    Service.mk { _ => Future.never }

  val totalTimeoutStack: Stack[ServiceFactory[Int, Int]] = {
    val svcFactory = ServiceFactory.const(neverSvc)
    // use a no-op module to verify it will get swapped out
    val totalModule = new NoOpModule[ServiceFactory[Int, Int]](
      TimeoutFilter.totalTimeoutRole, "testing total timeout")
    totalModule.toStack(Stack.Leaf(Stack.Role("test"), svcFactory))
  }

  val perReqTimeoutStack: Stack[ServiceFactory[Int, Int]] = {
    val svcFactory = ServiceFactory.const(neverSvc)
    TimeoutFilter.clientModule[Int, Int]
      .toStack(Stack.Leaf(Stack.Role("test"), svcFactory))
  }

  case class TestStackClient(
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
}

@RunWith(classOf[JUnitRunner])
class MethodBuilderTest
  extends FunSuite
  with Matchers
  with Eventually
  with IntegrationPatience {

  import MethodBuilderTest._

  test("retries do not see the total timeout") {
    val stats = new InMemoryStatsReceiver()
    val params =
      Stack.Params.empty +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)
    val stackClient = TestStackClient(totalTimeoutStack, params)
    val methodBuilder = MethodBuilder.from("retry_it", stackClient)

    val client = methodBuilder
      .withTimeout.total(10.milliseconds)
      .withRetry.forClassifier {
        case ReqRep(_, Throw(_: GlobalRequestTimeoutException)) =>
          ResponseClass.RetryableFailure
      }
      .newService("a_client")

    intercept[GlobalRequestTimeoutException] {
      Await.result(client(1), 5.seconds)
    }
    // while we have a RetryFilter, the underlying service returns `Future.never`
    // and as such, the stats are never updated.
    assert(stats.stat("retry_it", "a_client", "retries")() == Seq.empty)
  }

  test("per-request, retries, and total timeouts") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer()
    val params =
      Stack.Params.empty +
        param.Timer(timer) +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)

    val perReqTimeout = 50.milliseconds
    val totalTimeout = perReqTimeout * 2 + 20.milliseconds
    val svc: Service[Int, Int] = Service.mk { i =>
      Future.sleep(perReqTimeout + 1.millis)(timer).map(_ => i)
    }

    val stack = TimeoutFilter.clientModule[Int, Int]
      .toStack(Stack.Leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("together", stackClient)

    // the first 2 attempts will hit the per-request timeout, with each
    // being retried. then the the 3 attempt (2nd retry) should run into
    // the total timeout.
    val client = methodBuilder
      .withTimeout.perRequest(perReqTimeout)
      .withTimeout.total(totalTimeout)
      .withRetry.forClassifier {
        case ReqRep(_, Throw(_: IndividualRequestTimeoutException)) =>
          ResponseClass.RetryableFailure
      }
      .newService("a_client")

    Time.withCurrentTimeFrozen { tc =>
      // issue the request
      val rep = client(1)
      assert(!rep.isDefined)

      // hit the 1st per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()
      assert(!rep.isDefined)

      // hit the 2nd per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()
      assert(!rep.isDefined)

      // hit the total timeout
      tc.advance(20.milliseconds)
      timer.tick()
      assert(rep.isDefined)

      intercept[GlobalRequestTimeoutException] {
        Await.result(rep, 5.seconds)
      }

      eventually {
        // confirm there were 2 retries issued
        assert(stats.stat("together", "a_client", "retries")() == Seq(2))
      }
    }
  }

  test("logical stats") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer()
    val clientLabel = "the_client"
    val params =
      Stack.Params.empty +
        param.Label(clientLabel) +
        param.Timer(timer) +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)

    val perReqTimeout = 50.milliseconds
    val attempts = new AtomicInteger(0)
    val svc: Service[Int, Int] = Service.mk { i =>
      val num = attempts.incrementAndGet()
      if (num <= 1)
        Future.value(i).delayed(perReqTimeout + 1.millis)(timer)
      else
        Future.value(i)
    }

    val stack = TimeoutFilter.clientModule[Int, Int]
      .toStack(Stack.Leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("destination", stackClient)

    // the first attempts will hit the per-request timeout and will be
    // retried. then the retry should succeed.
    val methodName = "a_method"
    val client = methodBuilder
      .withTimeout.perRequest(perReqTimeout)
      .withRetry.forClassifier {
        case ReqRep(_, Throw(_: IndividualRequestTimeoutException)) =>
          ResponseClass.RetryableFailure
      }
      .newService(methodName)

    Time.withCurrentTimeFrozen { tc =>
      // issue the request
      val rep = client(1)
      assert(!rep.isDefined)

      // hit the 1st per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()

      // then let the retry go which should immediately succeed.
      val delta = 5.milliseconds
      tc.advance(delta)
      timer.tick()
      Await.ready(rep, 5.seconds)

      assert(2 == attempts.get)
      eventually {
        // confirm there was 1 retry
        assert(stats.stat(clientLabel, methodName, "retries")() == Seq(1))

        // the logical stats should only show 1 successful request despite
        // 2 "actual" requests
        assert(stats.counter(clientLabel, methodName, "logical", "requests")() == 1)
        assert(stats.counter(clientLabel, methodName, "logical", "success")() == 1)
        val latencies = stats.stat(clientLabel, methodName, "logical", "request_latency_ms")()
        assert(latencies.size == 1)
        assert(latencies.head <= (perReqTimeout + delta).inMillis)
      }

      val otherMethod = "other"
      val client2 = methodBuilder.newService(otherMethod)

      // issue the request
      val rep2 = client2(2)
      Await.ready(rep2, 5.seconds)

      assert(3 == attempts.get)
      eventually {
        // the logical stats should be separate per-"method"
        assert(stats.counter(clientLabel, otherMethod, "logical", "requests")() == 1)
      }
    }
  }
}
