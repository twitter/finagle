package com.twitter.finagle.service

import com.twitter.util.TimeConversions._
import com.twitter.util._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
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
    val timeoutFilter = new TimeoutFilter[Unit, Option[Deadline]](timeout, exception, timer)
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
