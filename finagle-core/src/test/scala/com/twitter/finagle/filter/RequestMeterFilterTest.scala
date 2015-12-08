package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncMeter
import com.twitter.conversions.time._
import com.twitter.finagle.{Failure, Service}
import com.twitter.util._

import java.util.concurrent.RejectedExecutionException

import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class RequestMeterFilterTest extends FunSuite with MockitoSugar {

  val echoSvc = new Service[Int, Int] {
    def apply(req: Int) = Future(req)
  }

  test("return service execution after getting a permit") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val meter = AsyncMeter.perSecond(1, 1)(timer)
      val svc = new RequestMeterFilter(meter).andThen(echoSvc)

      assert(Await.result(svc(1)) == 1)
    }
  }

  test("mark dropped requests as failed") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val meter = AsyncMeter.perSecond(1, 1)(timer)
      val svc = new RequestMeterFilter(meter).andThen(echoSvc)

      val f1 = svc(1)
      assert(f1.isDefined)

      val f2 = svc(2)
      assert(!f2.isDefined)

      val f3 = svc(3)
      assert(f3.isDefined)
      val failure = intercept[Failure] { Await.result(f3, 5.seconds) }
      intercept[RejectedExecutionException] { throw failure.getCause }

      ctl.advance(1.second)
      timer.tick()

      assert(f2.isDefined)
    }
  }

  test("meter exceptions are not wrapped as rejected") {
    val meter = mock[AsyncMeter]
    when(meter.await(1)).thenReturn(Future.exception(new RuntimeException("Error!")))

    Time.withCurrentTimeFrozen { ctl =>
      val svc = new RequestMeterFilter(meter).andThen(echoSvc)

      val f1 = svc(3)
      assert(f1.isDefined)
      val e = intercept[RuntimeException] { Await.result(f1, 5.seconds) }
      assert(e.getMessage == "Error!")
    }
  }

  test("service failures are not wrapped as rejected") {
    val timer = new MockTimer
    val exc = new Exception("app exc")
    val excSvc = new Service[Int, Int] {
      def apply(req: Int) = Future.exception(exc)
    }
    Time.withCurrentTimeFrozen { ctl =>
      val meter = AsyncMeter.perSecond(1, 1)(timer)
      val svc = new RequestMeterFilter(meter) andThen excSvc
      val e = intercept[Exception] { Await.result(svc(1)) }
      assert(e == exc)
    }

  }
}
