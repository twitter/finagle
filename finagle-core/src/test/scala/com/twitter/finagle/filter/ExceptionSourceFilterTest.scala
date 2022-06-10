package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.SourcedException
import com.twitter.finagle.Failure
import com.twitter.util.Await
import com.twitter.util.Future

import org.mockito.Matchers.anyInt
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ExceptionSourceFilterTest extends AnyFunSuite with MockitoSugar {
  test("ExceptionSourceFilter should add a name to sourced exceptions") {
    val service = mock[Service[Int, Int]]
    val e = new SourcedException {}
    when(service(anyInt)).thenReturn(Future.exception(e))
    val composed = new ExceptionSourceFilter("name", "appId") andThen service
    val actual = intercept[SourcedException] {
      Await.result(composed(0))
    }
    assert(actual.serviceName == "name")
  }

  test("ExceptionSourceFilter should add a name to failures") {
    val service = mock[Service[Int, Int]]
    val e = new Failure("everything sucks")
    when(service(anyInt)).thenReturn(Future.exception(e))
    val composed = new ExceptionSourceFilter("name", "appId") andThen service
    val actual = intercept[Failure] {
      Await.result(composed(0))
    }
    assert(actual.getSource(Failure.Source.Service) == Some("name"))
  }
}
