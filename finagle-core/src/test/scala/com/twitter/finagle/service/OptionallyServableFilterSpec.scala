package com.twitter.finagle.service

import com.twitter.finagle.{NotServableException, Service}
import com.twitter.util.{Await, Future}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class OptionallyServableFilterSpec extends SpecificationWithJUnit with Mockito {
  "OptionallyServableFilter" should {
    val underlying = mock[Service[String, String]]
    underlying.close(any) returns Future.Done

    val fn = mock[String => Future[Boolean]]
    val service = new OptionallyServableFilter(fn) andThen underlying
    val request = "request"
    val response = Future.value("response")
    "passes through when fn returns true" in {
      fn.apply(request) returns Future.value(true)

      underlying(request) returns response
      Await.result(service(request)) mustEqual Await.result(response)

      there was one(fn).apply(request)
    }

    "throws NotServableException when fn returns false" in {
      fn.apply(request) returns Future.value(false)

      Await.result(service(request)) must throwA[NotServableException]

      there was no(underlying).apply(any[String])
      there was one(fn).apply(request)
    }
  }
}
