package com.twitter.finagle.service

import com.twitter.util.TimeConversions._
import com.twitter.util._
import com.twitter.finagle.{Deadline, IndividualRequestTimeoutException, Service}
import com.twitter.finagle.context.Contexts
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class TimeoutFilterTest extends FunSuite with MockitoSugar {

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
      def apply(req: Unit) = Future.value(Contexts.broadcast.get(Deadline))
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
      assert(Await.result(timeoutService()) == Some(Deadline(Time.now, Time.now+1.second)))

      // Adjust existing ones.
      val f = Contexts.broadcast.let(Deadline, Deadline(Time.now-1.second, Time.now+200.milliseconds)) {
        timeoutService()
      }
      assert(Await.result(f) == Some(Deadline(Time.now, Time.now+200.milliseconds)))
    }
  }

  test("deadlines, infinite timeout") {
    val ctx = new DeadlineCtx(Duration.Top)
    import ctx._

    Time.withCurrentTimeFrozen { tc =>
      assert(Await.result(timeoutService()) == Some(Deadline(Time.now, Time.Top)))

      // Adjust existing ones
      val f = Contexts.broadcast.let(Deadline, Deadline(Time.now-1.second, Time.now+1.second)) {
        timeoutService()
      }
      assert(Await.result(f) == Some(Deadline(Time.now, Time.now+1.second)))
    }
  }
}
