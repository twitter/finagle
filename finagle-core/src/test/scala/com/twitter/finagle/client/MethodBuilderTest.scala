package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.{NoOpModule, Params}
import com.twitter.finagle._
import com.twitter.finagle.service.{Retries, RetryBudget, TimeoutFilter}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
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
