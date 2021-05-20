package com.twitter.finagle.service

import com.twitter.finagle.Service
import com.twitter.conversions.DurationOps._
import com.twitter.util.{Await, Future, Throw, Try}
import org.scalatest.funsuite.AnyFunSuite

class ServiceTest extends AnyFunSuite {
  test("Service should rescue") {
    val e = new RuntimeException("yargs")
    val exceptionThrowingService = new Service[Int, Int] {
      def apply(request: Int): Future[Int] = {
        throw e
      }
    }

    assert(Try(Await.result(Service.rescue(exceptionThrowingService)(1), 1.second)) == Throw(e))
  }
}
