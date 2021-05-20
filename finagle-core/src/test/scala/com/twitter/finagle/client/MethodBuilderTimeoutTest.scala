package com.twitter.finagle.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.client.MethodBuilderTest._
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.{
  GlobalRequestTimeoutException,
  IndividualRequestTimeoutException,
  RequestTimeoutException,
  ServiceFactory,
  Stack,
  param
}
import com.twitter.util.{Await, Duration, Future, MockTimer, Time, TimeControl}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MethodBuilderTimeoutTest
    extends AnyFunSuite
    with Matchers
    with Eventually
    with IntegrationPatience {

  private[this] val timer = new MockTimer()

  private val totalTimeoutExn = classOf[GlobalRequestTimeoutException]
  private val perReqTimeoutExn = classOf[IndividualRequestTimeoutException]

  private def totalTimeoutParams(timeout: Duration): Stack.Params = {
    Stack.Params.empty +
      TimeoutFilter.TotalTimeout(timeout) +
      param.Timer(timer) +
      param.Stats(NullStatsReceiver)
  }

  private def perReqTimeoutParams(timeout: Duration): Stack.Params = {
    Stack.Params.empty +
      TimeoutFilter.Param(timeout) +
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

    try Await.result(result, 1.second)
    catch {
      case ex: RequestTimeoutException =>
        assert(expectedException == ex.getClass)
        ex.getMessage should include(timeout.toString)
      case t: Throwable => fail(t)
    }
  }

  private def testTotalTimeout(stack: Stack[ServiceFactory[Int, Int]]): Unit = {
    // this is the default if a method doesn't override
    val stackClientTimeout = 4.seconds
    val params = totalTimeoutParams(stackClientTimeout)
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

    // verify that the timeout can be disabled
    // and the total timeout set on the StackClient has been removed
    val noTimeout = methodBuilder.withTimeout
      .total(Duration.Top)
      .newService("no_timeout")
    Time.withCurrentTimeFrozen { tc =>
      val result = noTimeout(1)

      // advancing past the default timeout should not trigger a timeout
      tc.advance(stackClientTimeout + 1.second)
      timer.tick()
      assert(!result.isDefined)
    }
  }

  test("total with module in stack") {
    testTotalTimeout(totalTimeoutStack)
  }

  test("total with module not in stack") {
    testTotalTimeout(totalTimeoutStack.remove(TimeoutFilter.totalTimeoutRole))
  }

  test("perRequest") {
    // this is the default if a method doesn't override
    val stackClientTimeout = 4.seconds
    val params = perReqTimeoutParams(stackClientTimeout)

    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("dest_paradise", stackClient)

    val fourSecs = methodBuilder.newService("4_secs")
    val twoSecs = methodBuilder.withTimeout.perRequest(2.seconds).newService("2_secs")
    val sixSecs = methodBuilder.withTimeout.perRequest(6.seconds).newService("6_secs")

    // no method-specific override, then a timeout is only supported
    // if the stack originally has the module
    Time.withCurrentTimeFrozen { tc =>
      assertBeforeAndAfterTimeout(fourSecs(1), 4.seconds, tc, perReqTimeoutExn)
    }

    // using a shorter timeout
    Time.withCurrentTimeFrozen { tc =>
      assertBeforeAndAfterTimeout(twoSecs(1), 2.seconds, tc, perReqTimeoutExn)
    }

    // using a longer timeout
    Time.withCurrentTimeFrozen { tc =>
      assertBeforeAndAfterTimeout(sixSecs(1), 6.seconds, tc, perReqTimeoutExn)
    }

    // verify that the timeout can be disabled
    // and the per-request timeout set on the StackClient has been removed
    val noTimeout = methodBuilder.withTimeout
      .perRequest(Duration.Top)
      .newService("no_timeout")
    Time.withCurrentTimeFrozen { tc =>
      val result = noTimeout(1)

      // advancing past the default timeout should not trigger a timeout
      tc.advance(stackClientTimeout + 1.second)
      timer.tick()
      assert(!result.isDefined)
    }
  }

}
