package com.twitter.finagle.filter

import com.twitter.finagle.SourcedException
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}

import org.junit.runner.RunWith

import org.mockito.Matchers.anyInt
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ExceptionSourceFilterTest extends FunSpec with MockitoSugar {
  describe("ExceptionSourceFilter") {
    it("should add a name to sourced exceptions") {
      val service = mock[Service[Int, Int]]
      val e = new SourcedException{}
      when(service(anyInt)).thenReturn(Future.exception(e))
      val composed = new ExceptionSourceFilter("name") andThen service
      val actual = intercept[SourcedException] {
        Await.result(composed(0))
      }
      assert(actual.serviceName === "name")
    }
  }
}
