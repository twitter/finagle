package com.twitter.finagle.service

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{verify, when}
import org.mockito.Matchers._
import com.twitter.finagle.{CancelledConnectionException, Service}
import com.twitter.util._

@RunWith(classOf[JUnitRunner])
class ProxyServiceTest extends FunSuite with MockitoSugar {
  class ProxyServiceHelper {
    val underlying = mock[Service[Int, Int]]
    when(underlying.close(any)) thenReturn Future.Done
  }

  test("ProxyService should proxy all methods to the underlying service") {
    val h = new ProxyServiceHelper
    import h._

    val proxy = new ProxyService(Future.value(underlying))

    val future = mock[Future[Int]]
    when(underlying(123)) thenReturn future

    assert(proxy(123) === future)
    verify(underlying)(123)

    when(underlying.isAvailable) thenReturn false
    assert(proxy.isAvailable === false)
    verify(underlying).isAvailable

    proxy.close()
    verify(underlying).close(any)
  }

  test("ProxyService should buffer requests") {
    val h = new ProxyServiceHelper
    import h._

    val promise = new Promise[Service[Int, Int]]
    val proxy = new ProxyService(promise)

    val f123 = proxy(123)
    val f321 = proxy(321)

    assert(!f123.isDefined)
    assert(!f321.isDefined)

    when(underlying(123)) thenReturn Future.value(111)
    when(underlying(321)) thenReturn Future.value(222)

    promise() = Return(underlying)

    assert(f123.isDefined)
    assert(f321.isDefined)

    assert(Await.result(f123) === 111)
    assert(Await.result(f321) === 222)
  }

  test("ProxyService should fail requests when underlying service provision fails") {
    val h = new ProxyServiceHelper

    val promise = new Promise[Service[Int, Int]]
    val proxy = new ProxyService(promise)
    val f = proxy(123)

    promise() = Throw(new Exception("sad panda"))
    assert(f.isDefined)
    intercept[Exception] {
      Await.result(f)
    }
  }

  test("ProxyService should proxy cancellation") {
    val h = new ProxyServiceHelper
    import h._

    val promise = new Promise[Service[Int, Int]]
    val proxy = new ProxyService(promise)
    val f123 = proxy(123)
    val replyPromise = new Promise[Int]
    when(underlying(123)) thenReturn replyPromise

    f123.raise(new Exception)

    promise() = Return(underlying)

    assert(f123.isDefined)
    assert(!replyPromise.isDefined)
    intercept[CancelledConnectionException] {
      Await.result(f123)
    }
  }
}
