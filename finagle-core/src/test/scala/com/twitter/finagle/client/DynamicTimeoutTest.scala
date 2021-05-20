package com.twitter.finagle.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.{Retries, RetryPolicy, TimeoutFilter}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.util._
import com.twitter.util.tunable.Tunable
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DynamicTimeoutTest
    extends AnyFunSuite
    with Matchers
    with Eventually
    with IntegrationPatience {

  private[this] val timer = new MockTimer()

  private def mkSvc(): Service[Int, Int] =
    Service.mk { _ => Future.never }

  private val perReqStack: Stack[ServiceFactory[Int, Int]] = {
    val svc = mkSvc()
    val svcFactory = ServiceFactory.const(svc)
    DynamicTimeout
      .perRequestModule[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), svcFactory))
  }

  private val perReqExn = classOf[IndividualRequestTimeoutException]

  private val totalExn = classOf[GlobalRequestTimeoutException]

  private def perReqParams(timeout: Duration, compensation: Duration): Stack.Params = {
    Stack.Params.empty +
      TimeoutFilter.Param(timeout) +
      param.Timer(timer) +
      LatencyCompensation.Compensation(compensation) +
      param.Stats(NullStatsReceiver)
  }

  private def perReqParams(timeout: Tunable[Duration], compensation: Duration): Stack.Params = {
    Stack.Params.empty +
      TimeoutFilter.Param(timeout) +
      param.Timer(timer) +
      LatencyCompensation.Compensation(compensation) +
      param.Stats(NullStatsReceiver)
  }

  private def totalParams(timeout: Duration): Stack.Params = {
    Stack.Params.empty +
      TimeoutFilter.TotalTimeout(timeout) +
      TimeoutFilter.Param(timeout) +
      param.Timer(timer) +
      param.Stats(NullStatsReceiver)
  }

  private def totalParams(timeout: Tunable[Duration]): Stack.Params = {
    Stack.Params.empty +
      TimeoutFilter.TotalTimeout(timeout) +
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

  test("per-request module uses default timeout when key not set") {
    val params = perReqParams(4.seconds, Duration.Undefined)
    val svc = Await.result(perReqStack.make(params).apply(ClientConnection.nil), 5.seconds)

    Time.withCurrentTimeFrozen { tc =>
      val res = svc(1)
      assertBeforeAndAfterTimeout(res, 4.seconds, tc, perReqExn)
    }
  }

  test("per-request module uses default tunable timeout when key not set") {
    val tunable = Tunable.mutable[Duration]("id", 4.seconds)
    val params = perReqParams(tunable, Duration.Undefined)
    val svc = Await.result(perReqStack.make(params).apply(ClientConnection.nil), 5.seconds)

    Time.withCurrentTimeFrozen { tc =>
      val res1 = svc(1)
      assertBeforeAndAfterTimeout(res1, 4.seconds, tc, perReqExn)

      // Make sure we're getting the new value once we set the Tunable
      tunable.set(2.seconds)

      val res2 = svc(1)
      assertBeforeAndAfterTimeout(res2, 2.seconds, tc, perReqExn)
    }
  }

  test("totalFilter uses default timeout when key not set") {
    val params = totalParams(4.seconds)
    val svc = DynamicTimeout.totalFilter(params).andThen(mkSvc())

    Time.withCurrentTimeFrozen { tc =>
      val res = svc(1)
      assertBeforeAndAfterTimeout(res, 4.seconds, tc, totalExn)
    }
  }

  test("totalFilter uses default tunable timeout when key not set") {
    val tunable = Tunable.mutable[Duration]("id", 4.seconds)
    val params = totalParams(tunable)
    val svc = DynamicTimeout.totalFilter(params).andThen(mkSvc())

    Time.withCurrentTimeFrozen { tc =>
      val res1 = svc(1)
      assertBeforeAndAfterTimeout(res1, 4.seconds, tc, totalExn)

      // Make sure we're getting the new value once we set the Tunable
      tunable.set(2.seconds)

      val res2 = svc(1)
      assertBeforeAndAfterTimeout(res2, 2.seconds, tc, totalExn)
    }
  }

  test("per-request module uses key's timeout when set") {
    val params = perReqParams(4.seconds, Duration.Undefined)
    val svc = Await.result(perReqStack.make(params).apply(ClientConnection.nil), 5.seconds)

    Time.withCurrentTimeFrozen { tc =>
      DynamicTimeout.letPerRequestTimeout(8.seconds) {
        val res = svc(1)
        assertBeforeAndAfterTimeout(res, 8.seconds, tc, perReqExn)
      }
    }
  }

  test("totalFilter uses key's timeout when set") {
    val params = totalParams(4.seconds)
    val svc = DynamicTimeout.totalFilter(params).andThen(mkSvc())

    Time.withCurrentTimeFrozen { tc =>
      DynamicTimeout.letTotalTimeout(8.seconds) {
        val res = svc(1)
        assertBeforeAndAfterTimeout(res, 8.seconds, tc, totalExn)
      }
    }
  }

  test("per-request module no timeout used when timeouts are not finite") {
    val params = perReqParams(4.seconds, Duration.Undefined)
    val svc = Await.result(perReqStack.make(params).apply(ClientConnection.nil), 5.seconds)

    Time.withCurrentTimeFrozen { tc =>
      DynamicTimeout.letPerRequestTimeout(Duration.Top) {
        val res = svc(1)
        tc.advance(1000.minutes)
        timer.tick()
        assert(!res.isDefined)
      }
    }
  }

  test("totalFilter no timeout used when timeouts are not finite") {
    val params = totalParams(4.seconds)
    val svc = DynamicTimeout.totalFilter(params).andThen(mkSvc())

    Time.withCurrentTimeFrozen { tc =>
      DynamicTimeout.letTotalTimeout(Duration.Top) {
        val res = svc(1)
        tc.advance(1000.minutes)
        timer.tick()
        assert(!res.isDefined)
      }
    }
  }

  test("per-request module default timeouts include latency compensation") {
    val params = perReqParams(4.seconds, 10.seconds)
    val svc = Await.result(perReqStack.make(params).apply(ClientConnection.nil), 5.seconds)

    Time.withCurrentTimeFrozen { tc =>
      val res = svc(1)
      assertBeforeAndAfterTimeout(res, 14.seconds, tc, perReqExn)
    }
  }

  test("per-request module overridden timeouts include latency compensation") {
    val params = perReqParams(4.seconds, 10.seconds)
    val svc = Await.result(perReqStack.make(params).apply(ClientConnection.nil), 5.seconds)

    Time.withCurrentTimeFrozen { tc =>
      DynamicTimeout.letPerRequestTimeout(2.seconds) {
        val res = svc(1)
        assertBeforeAndAfterTimeout(res, 12.seconds, tc, perReqExn)
      }
    }
  }

  test("simulated per-request and total timeouts uses timeouts when specified") {
    val stats = new InMemoryStatsReceiver()

    // 1 retry on a per-request timeout
    val retryOnTimeout = RetryPolicy.tries[Try[Nothing]](
      2,
      {
        case Throw(_: IndividualRequestTimeoutException) => true
      })

    val params =
      totalParams(1500.millis) ++
        perReqParams(1000.millis, Duration.Undefined) +
        HighResTimer(timer) +
        Retries.Policy(retryOnTimeout) +
        param.Stats(stats)

    val svrSvc = Service.mk { _: Int => new Promise[Int]() }
    val svcFactory = ServiceFactory.const(svrSvc)

    // build a stack with requests flowing top to bottom (though StackBuilder.push
    // works such that you push bottom up).
    //   - TotalTimeout
    //   - Retries
    //   - PerRequestTimeout
    //   - Service
    val stackBuilder =
      new StackBuilder[ServiceFactory[Int, Int]](Stack.leaf(Stack.Role("test"), svcFactory))
    stackBuilder.push(DynamicTimeout.perRequestModule)
    stackBuilder.push(Retries.moduleWithRetryPolicy)
    val stackSvc =
      Await.result(stackBuilder.result.make(params).apply(ClientConnection.nil), 5.seconds)
    val svc = DynamicTimeout.totalFilter(params).andThen(stackSvc)

    Time.withCurrentTimeFrozen { tc =>
      DynamicTimeout.letTotalTimeout(150.millis) {
        DynamicTimeout.letPerRequestTimeout(100.millis) {
          val res = svc(1)

          // not yet at the per-req timeout
          tc.advance(99.millis)
          timer.tick()
          assert(!res.isDefined)

          // advance past the per-request timeout which should trigger a retry
          // and therefore the response is not satisfied yet
          tc.advance(2.millis)
          timer.tick()
          assert(!res.isDefined)

          // advance past the total timeout triggering that.
          tc.advance(50.millis)
          timer.tick()
          assert(res.isDefined)

          intercept[GlobalRequestTimeoutException] {
            Await.result(res, 1.second)
          }

          // trigger the retried request's timeout so that we can validate
          // the retry stats
          tc.advance(100.millis)
          timer.tick()
          eventually {
            assert(Seq(1) == stats.stat("retries")())
          }
        }
      }
    }
  }

}
