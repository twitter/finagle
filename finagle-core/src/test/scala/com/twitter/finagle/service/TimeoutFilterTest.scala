package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.service.TimeoutFilterTest.TunableTimeoutHelper
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.SpanId
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.TraceId
import com.twitter.util._
import com.twitter.util.tunable.Tunable
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.TimeoutException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import scala.language.reflectiveCalls

private object TimeoutFilterTest {

  class TimeoutFilterHelper {
    val timer = new MockTimer
    val promise = new Promise[String] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler {
        case exc => interrupted = Some(exc)
      }
    }
    val service = new Service[String, String] {
      def apply(request: String) = promise
    }
    val timeout = 1.second
    val exception = new IndividualRequestTimeoutException(timeout)
    val timeoutFilter = new TimeoutFilter[String, String](timeout, exception, timer)
    val timeoutService = timeoutFilter.andThen(service)
  }

  class TunableTimeoutHelper {
    val timer = new MockTimer()
    val tunable = Tunable.emptyMutable[Duration]("id")

    val svc = Service.mk { _: String => Future.never }
    val svcFactory = ServiceFactory.const(svc)
    val stack = TimeoutFilter
      .clientModule[String, String]
      .toStack(Stack.leaf(Stack.Role("test"), svcFactory))

    val params = Stack.Params.empty + param.Timer(timer) + TimeoutFilter.Param(tunable)
    val service = stack.make(params).toService
  }
}

class TimeoutFilterTest extends AnyFunSuite with Matchers with MockitoSugar {

  import TimeoutFilterTest.TimeoutFilterHelper

  test("TimeoutFilter should request succeeds when the service succeeds") {
    val h = new TimeoutFilterHelper
    import h._

    promise.setValue("1")
    val res = timeoutService("blah")
    assert(res.isDefined)
    assert(Await.result(res) == "1")
  }

  test("TimeoutFilter should time out a request that is not successful, cancels underlying") {
    val h = new TimeoutFilterHelper
    import h._

    Time.withCurrentTimeFrozen { tc: TimeControl =>
      val res = timeoutService("blah")
      assert(!res.isDefined)
      assert(promise.interrupted == None)
      tc.advance(2.seconds)
      timer.tick()
      assert(res.isDefined)
      val t = promise.interrupted
      intercept[RequestTimeoutException] {
        throw t.get
      }
      intercept[IndividualRequestTimeoutException] {
        Await.result(res)
      }
    }
  }

  test("TimeoutFilter should not transform a TimeoutException") {
    val timeoutException = new TimeoutException("timeout exception")
    val service = new Service[Unit, Unit] {
      def apply(req: Unit) = throw timeoutException
    }

    val timeoutFilter = new TimeoutFilter[Unit, Unit](1.second, new MockTimer)
    val timeoutService = timeoutFilter.andThen(service)

    val exc = intercept[TimeoutException] {
      Await.result(timeoutService())
    }
    assert(exc == timeoutException)
  }

  class DeadlineCtx(val timeout: Duration) {
    val service = new Service[Unit, Option[Deadline]] {
      def apply(req: Unit) = Future.value(Deadline.current)
    }

    val timer = new MockTimer
    val exception = new IndividualRequestTimeoutException(timeout)
    val timeoutFilter = new TimeoutFilter[Unit, Option[Deadline]](timeout, exception, timer)
    val timeoutService = timeoutFilter andThen service
  }

  test("deadlines, finite timeout") {
    val ctx = new DeadlineCtx(1.second)
    import ctx._

    Time.withCurrentTimeFrozen { tc =>
      assert(
        Await.result(timeoutService((): Unit)) == Some(Deadline(Time.now, Time.now + 1.second))
      )

      // Adjust existing ones.
      val f = Contexts.broadcast
        .let(Deadline, Deadline(Time.now - 1.second, Time.now + 200.milliseconds)) {
          timeoutService((): Unit)
        }
      assert(Await.result(f) == Some(Deadline(Time.now, Time.now + 200.milliseconds)))
    }
  }

  test("deadlines, infinite timeout") {
    val ctx = new DeadlineCtx(Duration.Top)
    import ctx._

    Time.withCurrentTimeFrozen { tc =>
      assert(Await.result(timeoutService((): Unit)) == Some(Deadline(Time.now, Time.Top)))

      // Adjust existing ones
      val f = Contexts.broadcast.let(Deadline, Deadline(Time.now - 1.second, Time.now + 1.second)) {
        timeoutService((): Unit)
      }
      assert(Await.result(f) == Some(Deadline(Time.now, Time.now + 1.second)))
    }
  }

  test("bug verification: TimeoutFilter incorrectly sends expired deadlines") {
    val ctx = new DeadlineCtx(1.second)

    import ctx._

    Time.withCurrentTimeFrozen { tc =>
      val now = Time.now
      val f = Contexts.broadcast.let(Deadline, Deadline(now, now + 1.second)) {
        tc.advance(5.seconds)
        timeoutService((): Unit)
      }
      assert(Await.result(f) == Some(Deadline(now + 5.seconds, now + 1.second)))
    }
  }

  private def verifyFilterAddedOrNot(timeoutModule: Stackable[ServiceFactory[Int, Int]]) = {
    val svc = Service.mk { i: Int => Future.value(i) }
    val svcFactory = ServiceFactory.const(svc)
    val stack = timeoutModule.toStack(Stack.leaf(Stack.Role("test"), svcFactory))

    def assertNoTimeoutFilter(duration: Duration): Unit = {
      val params = Stack.Params.empty + TimeoutFilter.Param(duration)
      val made = stack.make(params)
      // this relies on the fact that we do not compose
      // with a TimeoutFilter if the duration is not appropriate.
      assert(svcFactory eq made)
    }
    assertNoTimeoutFilter(Duration.Bottom)
    assertNoTimeoutFilter(Duration.Top)
    assertNoTimeoutFilter(Duration.Undefined)
    assertNoTimeoutFilter(Duration.Zero)
    assertNoTimeoutFilter(-1.second)

    def assertNoTimeoutFilterTunable(tunable: Tunable[Duration]): Unit = {
      val params = Stack.Params.empty + TimeoutFilter.Param(tunable)
      val made = stack.make(params)
      assert(svcFactory eq made)
    }

    assertNoTimeoutFilterTunable(Tunable.const("id", Duration.Bottom))
    assertNoTimeoutFilterTunable(Tunable.const("id", Duration.Top))
    assertNoTimeoutFilterTunable(Tunable.const("id", Duration.Undefined))
    assertNoTimeoutFilterTunable(Tunable.const("id", Duration.Zero))
    assertNoTimeoutFilterTunable(Tunable.const("id", -1.second))

    def assertTimeoutFilter(duration: Duration): Unit = {
      val params = Stack.Params.empty + TimeoutFilter.Param(duration)
      val made = stack.make(params)
      // this relies on the fact that we do compose
      // with a TimeoutFilter if the duration is appropriate.
      assert(svcFactory ne made)
    }
    assertTimeoutFilter(10.seconds)

    def assertTimeoutFilterTunable(tunable: Tunable[Duration]): Unit = {
      val params = Stack.Params.empty + TimeoutFilter.Param(tunable)
      val made = stack.make(params)
      assert(svcFactory ne made)
    }
    assertTimeoutFilterTunable(Tunable.const("id", 10.seconds))

    assertTimeoutFilterTunable(Tunable.emptyMutable[Duration]("undefined"))
    assertTimeoutFilterTunable(Tunable.mutable[Duration]("id", 10.seconds))
    assertTimeoutFilterTunable(Tunable.mutable[Duration]("id", Duration.Top))
  }

  test("filter added or not to clientModule based on duration") {
    verifyFilterAddedOrNot(TimeoutFilter.clientModule[Int, Int])
  }

  test("filter added or not to serverModule based on duration") {
    verifyFilterAddedOrNot(TimeoutFilter.serverModule[Int, Int])
  }

  def testTypeAgnostic(filter: TypeAgnostic, timer: MockTimer) = {
    val h = new TimeoutFilterHelper()
    val svc = filter.andThen(h.service)

    Time.withCurrentTimeFrozen { tc =>
      val res = svc("hello")
      assert(!res.isDefined)

      // not yet at the timeout
      tc.advance(4.seconds)
      timer.tick()
      assert(!res.isDefined)

      // go past the timeout
      tc.advance(2.seconds)
      timer.tick()
      intercept[IndividualRequestTimeoutException] {
        Await.result(res, 1.second)
      }
    }
  }

  test("typeAgnostic using timeout") {
    val timer = new MockTimer()
    val timeout = 5.seconds
    val filter =
      TimeoutFilter.typeAgnostic(timeout, new IndividualRequestTimeoutException(timeout), timer)

    testTypeAgnostic(filter, timer)
  }

  test("typeAgnostic using tunable timeout") {
    val timer = new MockTimer()
    val timeoutTunable = Tunable.const("myTimeout", 5.seconds)
    val filter = TimeoutFilter.typeAgnostic(
      timeoutTunable,
      timeout => new IndividualRequestTimeoutException(timeout),
      timer
    )

    testTypeAgnostic(filter, timer)
  }

  test("variable timeouts") {
    val timer = new MockTimer()
    val atomicTimeout = new AtomicReference[Duration](Duration.Top)
    val filter = new TimeoutFilter[String, String](
      () => atomicTimeout.get,
      timeout => new IndividualRequestTimeoutException(timeout),
      timer
    )

    val h = new TimeoutFilterHelper()
    val svc = filter.andThen(h.service)

    Time.withCurrentTimeFrozen { tc =>
      atomicTimeout.set(5.seconds)
      val res = svc("hello")
      assert(!res.isDefined)

      // not yet at the timeout
      tc.advance(4.seconds)
      timer.tick()
      assert(!res.isDefined)

      // go past the timeout
      tc.advance(2.seconds)
      timer.tick()
      val ex = intercept[IndividualRequestTimeoutException] {
        Await.result(res, 1.second)
      }
      ex.getMessage should include(atomicTimeout.get.toString)

      // change the timeout
      atomicTimeout.set(3.seconds)
      val res2 = svc("hello")
      assert(!res2.isDefined)

      // this time, 4 seconds pushes us past
      tc.advance(4.seconds)
      timer.tick()
      val ex2 = intercept[IndividualRequestTimeoutException] {
        Await.result(res2, 1.second)
      }
      ex2.getMessage should include(atomicTimeout.get.toString)
    }
  }

  test("variable timeout when timeout is not finite") {
    val timer = new MockTimer()
    val atomicTimeout = new AtomicReference[Duration]()
    val filter = new TimeoutFilter[String, String](
      () => atomicTimeout.get,
      timeout => new IndividualRequestTimeoutException(timeout),
      timer
    )

    val h = new TimeoutFilterHelper()
    val svc = filter.andThen(h.service)

    Time.withCurrentTimeFrozen { tc =>
      atomicTimeout.set(Duration.Undefined)
      val res = svc("hello")
      assert(!res.isDefined)

      tc.advance(200.seconds)
      timer.tick()

      assert(!res.isDefined)
    }
  }

  test("Tunable timeout: No timeout if Tunable not set") {
    val h = new TunableTimeoutHelper
    import h._

    Time.withCurrentTimeFrozen { tc =>
      // No timeout because Tunable uses Duration.Top if applying it produces `None`
      val res = service("hello")
      assert(!res.isDefined)
      tc.advance(1.hour)
      timer.tick()
      intercept[com.twitter.util.TimeoutException] {
        Await.result(res, 1.second)
      }
    }
  }

  test("Tunable timeouts: Timeout if tunable is set") {
    val h = new TunableTimeoutHelper
    import h._

    Time.withCurrentTimeFrozen { tc =>
      // set a timeout
      tunable.set(5.seconds)
      val res = service("hello")
      assert(!res.isDefined)

      // not yet at the timeout
      tc.advance(4.seconds)
      timer.tick()
      assert(!res.isDefined)

      // go past the timeout
      tc.advance(2.seconds)
      timer.tick()
      val ex = intercept[IndividualRequestTimeoutException] {
        Await.result(res, 1.second)
      }
      ex.getMessage should include(tunable().get.toString)
    }
  }

  test("Tunable timeouts: Timeout if tunable is updated") {
    val timer = new MockTimer()
    val timeoutTunable = Tunable.emptyMutable[Duration]("id")
    val filter = new TimeoutFilter[String, String](
      timeoutTunable,
      timeout => new IndividualRequestTimeoutException(timeout),
      timer
    )

    val h = new TimeoutFilterHelper()
    val svc = filter.andThen(h.service)

    Time.withCurrentTimeFrozen { tc =>
      timeoutTunable.set(5.seconds)
      val res = svc("hello")
      assert(!res.isDefined)

      // not yet at the timeout
      tc.advance(4.seconds)
      timer.tick()
      assert(!res.isDefined)

      // go past the timeout
      tc.advance(2.seconds)
      timer.tick()
      val ex = intercept[IndividualRequestTimeoutException] {
        Await.result(res, 1.second)
      }
      ex.getMessage should include(timeoutTunable().get.toString)

      // change the timeout
      timeoutTunable.set(3.seconds)
      val res2 = svc("hello")
      assert(!res2.isDefined)

      // this time, 4 seconds pushes us past
      tc.advance(4.seconds)
      timer.tick()
      val ex2 = intercept[IndividualRequestTimeoutException] {
        Await.result(res2, 1.second)
      }
      ex2.getMessage should include(timeoutTunable().get.toString)
    }
  }

  test("Tunable timeouts: No timeout if Tunable cleared") {
    val h = new TunableTimeoutHelper
    import h._

    Time.withCurrentTimeFrozen { tc =>
      // set the timeout
      tunable.set(3.seconds)
      val res = service("hello")
      assert(!res.isDefined)

      // 4 seconds pushes us past
      tc.advance(4.seconds)
      timer.tick()
      intercept[IndividualRequestTimeoutException] {
        Await.result(res, 1.second)
      }

      // clear the tunable; should use Duration.Top again
      tunable.clear()
      val res2 = service("hello")
      assert(!res2.isDefined)
      tc.advance(1.hour)
      timer.tick()
      intercept[com.twitter.util.TimeoutException] {
        Await.result(res2, 1.second)
      }
    }
  }

  test("can disable deadline propagation") {
    val service = new Service[String, Boolean] {
      // returns a Future of a Boolean indicating whether or not there is a deadline defined
      def apply(request: String): Future[Boolean] =
        Future.value(Contexts.broadcast.get(Deadline).isDefined)
    }

    val timer = new MockTimer
    val timeout = 1.second
    val exceptionFn: Duration => RequestTimeoutException =
      _ => new IndividualRequestTimeoutException(timeout)

    val propagationDisabledFilter =
      new TimeoutFilter[String, Boolean](() => timeout, exceptionFn, timer, false)
    val propagationDisabledService = propagationDisabledFilter.andThen(service)

    assert(Await.result(propagationDisabledService("foo")) == false)

    val propagationEnabledFilter =
      new TimeoutFilter[String, Boolean](() => timeout, exceptionFn, timer, true)
    val propagationEnabledService = propagationEnabledFilter.andThen(service)

    assert(Await.result(propagationEnabledService("bar")) == true)
  }

  private def toggleOnCtx(fn: => Unit): Unit = {
    DeadlineOnlyToggle.unsafeOverride(Some(true))
    try fn
    finally DeadlineOnlyToggle.unsafeOverride(None)
  }

  private def fakeTraceRecord(deadline: Deadline): Annotation.BinaryAnnotation = {
    val deadlineRecord = s"timestamp:${deadline.timestamp}:deadline:${deadline.deadline}"
    Annotation.BinaryAnnotation(
      TimeoutFilter.clientKey + TimeoutFilter.DeadlineAnnotation,
      s"deadline_enabled:$deadlineRecord")
  }

  test("only honor deadlines when DeadlineOnly toggled up") {
    val h = new TimeoutFilterHelper
    import h._

    val sr = new InMemoryStatsReceiver()
    // the 1.millis timeout should be suppressed by deadline
    val timeoutFilterWithPreferDeadline =
      new TimeoutFilter[String, String](
        () => 1.millisecond,
        _ => exception,
        timer,
        true,
        true,
        sr,
        TimeoutFilter.clientKey)
    val deadline = Deadline.ofTimeout(2.seconds)
    val tracer = new BufferingTracer

    Trace.letTracer(tracer) {
      toggleOnCtx {
        Time.withCurrentTimeFrozen { tc =>
          Contexts.broadcast.let(Deadline, deadline) {
            val result = timeoutFilterWithPreferDeadline.andThen(service)("foo")
            tc.advance(1.second)
            timer.tick()
            promise.setValue("done")
            assert(Await.result(result, 2.seconds) == "done")
          }

          assert(sr.stat("current_deadline")().size == 1)
          assert(sr.counter("deadline_only")() == 1)
          assert(sr.counter("deadline_lt_timeout_experiment")() == 0)

          assert(
            tracer.toSeq == Seq(
              Record(Trace.id, Time.now - 1.second, fakeTraceRecord(deadline), None)))
        }
      }
    }
  }

  test("strict setting suppress permissive setting") {
    val timer = new MockTimer
    val service = new Service[String, String] {
      def apply(request: String): Future[String] = Future.value("done")
    }
    val exceptionFn = { timeout: Duration =>
      new IndividualRequestTimeoutException(timeout)
    }

    val sr = new InMemoryStatsReceiver()
    val timeoutFilter =
      new TimeoutFilter[String, String](() => 1.second, exceptionFn, timer, true, false, sr)
    val deadline1 = Deadline.ofTimeout(2.seconds)
    val deadline2 = Deadline.ofTimeout(50.millis)

    val tracer = new BufferingTracer

    Time.withCurrentTimeFrozen { _ =>
      Trace.letTracer(tracer) {
        val traceContext1 = TraceId(Some(SpanId(0xabc)), None, SpanId(0x123), None)
        Trace.letId(traceContext1) {
          Contexts.broadcast.let(Deadline, deadline1) {
            assert(Await.result(timeoutFilter.andThen(service)("foo"), 2.seconds) == "done")
          }

          assert(sr.stat("current_deadline")().size == 1)
          assert(sr.counter("deadline_only")() == 0)
          assert(sr.counter("deadline_lt_timeout")() == 0)

          Contexts.broadcast.let(Deadline, deadline2) {
            assert(Await.result(timeoutFilter.andThen(service)("foo"), 2.seconds) == "done")
          }

          assert(sr.stat("current_deadline")().size == 2)
          assert(sr.counter("deadline_only")() == 0)
          assert(sr.counter("deadline_lt_timeout")() == 1)
        }
      }
    }

  }

}
