package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import com.twitter.util.{Future, Return, Throw}

import com.twitter.finagle.{Service, Filter, NotServableException}

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
      service(request)() mustEqual response()

      there was one(fn).apply(request)
    }

    "throws NotServableException when fn returns false" in {
      fn.apply(request) returns Future.value(false)

      service(request)() must throwA[NotServableException]

      there was no(underlying).apply(any[String])
      there was one(fn).apply(request)
    }
  }
}
