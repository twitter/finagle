package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncMeter
import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Failure, Service}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RequestMeterFilterTest extends FunSuite {

  val timer = DefaultTimer.twitter
  val meter = AsyncMeter.newMeter(1, 1 second, 1)(timer)

  test("return service execution after getting a permit") {
    val echoSvc = new Service[Int, Int] {
      def apply(req: Int) = Future(req)
    }
    val svc = new RequestMeterFilter(meter) andThen echoSvc
    assert(Await.result(svc(2)) == 2)
  }

  test("mark dropped requests as rejected") {
    val neverSvc = new Service[Int, Int] {
      def apply(req: Int) = Future.never
    }
    val svc = new RequestMeterFilter(meter, 2) andThen neverSvc
    val f = intercept[Failure] { Await.result(svc(1)) }
    assert(f.isFlagged(Failure.Restartable))
  }

  test("service failures are not wrapped as rejected") {
    val exc = new Exception("app exc")
    val excSvc = new Service[Int, Int] {
      def apply(req: Int) = Future.exception(exc)
    }
    val svc = new RequestMeterFilter(meter) andThen excSvc
    val e = intercept[Exception] { Await.result(svc(1)) }
    assert(e == exc)
  }
}
