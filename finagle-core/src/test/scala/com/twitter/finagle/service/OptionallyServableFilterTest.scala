package com.twitter.finagle.service

import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers._
import com.twitter.finagle.NotServableException
import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class OptionallyServableFilterTest extends AnyFunSuite with MockitoSugar {

  class OptionallyServableFilterHelper {
    val underlying = mock[Service[String, String]]
    when(underlying.close(any)) thenReturn Future.Done

    val fn = mock[String => Future[Boolean]]
    val service = new OptionallyServableFilter(fn) andThen underlying
    val request = "request"
    val response = Future.value("response")
  }

  test("OptionallyServableFilter should passes through when fn returns true") {
    val h = new OptionallyServableFilterHelper
    import h._

    when(fn.apply(request)) thenReturn Future.value(true)
    when(underlying(request)) thenReturn response
    assert(Await.result(service(request)) == Await.result(response))

    verify(fn).apply(request)
  }

  test("OptionallyServableFilter should throws NotServableException when fn returns false") {
    val h = new OptionallyServableFilterHelper
    import h._

    when(fn.apply(request)) thenReturn Future.value(false)

    intercept[NotServableException] {
      Await.result(service(request))
    }
    verify(underlying, times(0)).apply(any[String])
    verify(fn).apply(request)
  }

}
