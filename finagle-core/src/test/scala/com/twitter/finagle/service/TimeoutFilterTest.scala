package com.twitter.finagle.service

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.TimeConversions._
import com.twitter.util._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.tracing._
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{atLeastOnce, spy, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._
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
}

@RunWith(classOf[JUnitRunner])
class TimeoutFilterTest extends FunSuite with MockitoSugar {

  import TimeoutFilterTest.TimeoutFilterHelper

  test("TimeoutFilter should request succeeds when the service succeeds") {
    val h = new TimeoutFilterHelper
    import h._

    promise.setValue("1")
    val res = timeoutService("blah")
    assert(res.isDefined)
    assert(Await.result(res) == "1")
  }

  test("TimeoutFilter should times out a request that is not successful, cancels underlying") {
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
      intercept[java.util.concurrent.TimeoutException] {
        throw t.get
      }
      intercept[IndividualRequestTimeoutException] {
        Await.result(res)
      }
    }
  }

  class DeadlineCtx(val timeout: Duration) {
    val service = new Service[Unit, Option[Deadline]] {
      def apply(req: Unit) = Future.value(Deadline.current)
    }

    val timer = new MockTimer
    val exception = new IndividualRequestTimeoutException(timeout)
    val statsReceiver = new InMemoryStatsReceiver
    val timeoutFilter = new TimeoutFilter[Unit, Option[Deadline]](timeout, exception, timer, statsReceiver)
    val timeoutService = timeoutFilter andThen service
  }

  test("deadlines, finite timeout") {
    val ctx = new DeadlineCtx(1.second)
    import ctx._

    Time.withCurrentTimeFrozen { tc =>
      assert(Await.result(timeoutService((): Unit)) == Some(Deadline(Time.now, Time.now+1.second)))

      // Adjust existing ones.
      val f = Contexts.broadcast.let(Deadline, Deadline(Time.now-1.second, Time.now+200.milliseconds)) {
        timeoutService((): Unit)
      }
      assert(Await.result(f) == Some(Deadline(Time.now, Time.now+200.milliseconds)))
    }
  }

  test("deadlines, infinite timeout") {
    val ctx = new DeadlineCtx(Duration.Top)
    import ctx._

    Time.withCurrentTimeFrozen { tc =>
      assert(Await.result(timeoutService((): Unit)) == Some(Deadline(Time.now, Time.Top)))

      // Adjust existing ones
      val f = Contexts.broadcast.let(Deadline, Deadline(Time.now-1.second, Time.now+1.second)) {
        timeoutService((): Unit)
      }
      assert(Await.result(f) == Some(Deadline(Time.now, Time.now+1.second)))
    }
  }

  test("stats recorded") {
    val ctx = new DeadlineCtx(200.milliseconds)

    import ctx._

    Time.withCurrentTimeFrozen { tc =>
      val f = Contexts.broadcast.let(Deadline, Deadline(Time.now-1.second, Time.now+1.second)) {
        timeoutService((): Unit)
      }
      assert(Await.result(f) == Some(Deadline(Time.now, Time.now+200.milliseconds)))

      assert(statsReceiver.stats(Seq("timestamp_ms"))(0) == Time.now.inMillis)
      assert(statsReceiver.stats(Seq("timeout_ms"))(0) == 200)
      assert(statsReceiver.stats(Seq("incoming_deadline_ms"))(0) == 1.second.inMillis)
    }
  }

  test("bug verification: TimeoutFilter incorrectly sends expired deadlines") {
    val ctx = new DeadlineCtx(1.second)

    import ctx._

    Time.withCurrentTimeFrozen { tc =>
      val now = Time.now
      val f = Contexts.broadcast.let(Deadline, Deadline(now, now+1.second)) {
        tc.advance(5.seconds)
        timeoutService((): Unit)
      }
      assert(Await.result(f) == Some(Deadline(now + 5.seconds, now + 1.second)))

      assert(statsReceiver.stats(Seq("expired_deadline_ms"))(0) == 4.seconds.inMillis)
    }
  }

  test("trace recorded") {
    def withExpectedTrace(
      f: => Unit,
      expected: Seq[Annotation]
    ) {
      val tracer: Tracer = spy(new NullTracer)
      val captor: ArgumentCaptor[Record] = ArgumentCaptor.forClass(classOf[Record])
      Trace.letTracer(tracer) { f }
      verify(tracer, atLeastOnce()).record(captor.capture())
      val annotations = captor.getAllValues.asScala collect { case Record(_, _, a, _) => a }
      assert(expected == annotations)
    }

    Time.withCurrentTimeFrozen { tc =>
      withExpectedTrace({
        val ctx = new DeadlineCtx(200.milliseconds)
        import ctx._


        val f = Contexts.broadcast.let(Deadline, Deadline(Time.now-1.second, Time.now+1.second)) {
          timeoutService((): Unit)
        }
        assert(Await.result(f, 1.second) == Some(Deadline(Time.now, Time.now+200.milliseconds)))
      },
      Seq(
        Annotation.BinaryAnnotation("finagle.timeoutFilter.timeoutDeadline.timestamp_ms", Time.now.inMillis),
        Annotation.BinaryAnnotation("finagle.timeoutFilter.timeoutDeadline.deadline_ms", (Time.now+200.millis).inMillis),
        Annotation.BinaryAnnotation("finagle.timeoutFilter.incomingDeadline.timestamp_ms", (Time.now-1.second).inMillis),
        Annotation.BinaryAnnotation("finagle.timeoutFilter.incomingDeadline.deadline_ms", (Time.now+1.second).inMillis),
        Annotation.BinaryAnnotation("finagle.timeoutFilter.outgoingDeadline.timestamp_ms", Time.now.inMillis),
        Annotation.BinaryAnnotation("finagle.timeoutFilter.outgoingDeadline.deadline_ms", (Time.now+200.milliseconds).inMillis)
      ))
    }
  }

  private def verifyFilterAddedOrNot(
    timoutModule: Stackable[ServiceFactory[Int, Int]]
  ) = {
    val svc = Service.mk { i: Int => Future.value(i) }
    val svcFactory = ServiceFactory.const(svc)
    val stack = timoutModule.toStack(Stack.Leaf(Stack.Role("test"), svcFactory))

    def assertNoTimeoutFilter(duration: Duration): Unit = {
      val params = Stack.Params.empty + TimeoutFilter.Param(duration)
      val made = stack.make(params)
      // this relies on the fact that we do not compose
      // with a TimeoutFilter if the duration is not appropriate.
      assert(svcFactory == made)
    }
    assertNoTimeoutFilter(Duration.Bottom)
    assertNoTimeoutFilter(Duration.Top)
    assertNoTimeoutFilter(Duration.Undefined)
    assertNoTimeoutFilter(Duration.Zero)
    assertNoTimeoutFilter(-1.second)

    def assertTimeoutFilter(duration: Duration): Unit = {
      val params = Stack.Params.empty + TimeoutFilter.Param(duration)
      val made = stack.make(params)
      // this relies on the fact that we do compose
      // with a TimeoutFilter if the duration is appropriate.
      assert(svcFactory != made)
    }
    assertTimeoutFilter(10.seconds)
  }

  test("filter added or not to clientModule based on duration") {
    verifyFilterAddedOrNot(TimeoutFilter.clientModule[Int, Int])
  }

  test("filter added or not to serverModule based on duration") {
    verifyFilterAddedOrNot(TimeoutFilter.serverModule[Int, Int])
  }
}
