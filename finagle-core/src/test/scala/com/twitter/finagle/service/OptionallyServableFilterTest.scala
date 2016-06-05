package com.twitter.finagle.service

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import com.twitter.finagle.{NotServableException, Service}
import org.mockito.Mockito.{times, verify, when}
import org.mockito.Matchers._
import com.twitter.util.{Await, Future}

@RunWith(classOf[JUnitRunner])
class OptionallyServableFilterTest extends FunSuite with MockitoSugar {

  class OptionnallyServableFilterHelper {
    val underlying = mock[Service[String, String]]
    when(underlying.close(any)) thenReturn Future.Done

    val fn = mock[String => Future[Boolean]]
    val service = new OptionallyServableFilter(fn) andThen underlying
    val request = "request"
    val response = Future.value("response")
  }

  test("OptionallyServableFilter should passes through when fn returns true") {
    val h = new OptionnallyServableFilterHelper
    import h._

    when(fn.apply(request)) thenReturn Future.value(true)
    when(underlying(request)) thenReturn response
    assert(Await.result(service(request)) == Await.result(response))

    verify(fn).apply(request)
  }

  test("OptionallyServableFilter should throws NotServableException when fn returns false") {
    val h = new OptionnallyServableFilterHelper
    import h._

    when(fn.apply(request)) thenReturn Future.value(false)

    intercept[NotServableException] {
      Await.result(service(request))
    }
    verify(underlying, times(0)).apply(any[String])
    verify(fn).apply(request)
  }

}
