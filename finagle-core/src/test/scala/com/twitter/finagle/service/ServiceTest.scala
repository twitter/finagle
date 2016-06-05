package com.twitter.finagle.service

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.twitter.finagle.Service
import com.twitter.util.TimeConversions._
import com.twitter.util.{Throw, Await, Try, Future}

@RunWith(classOf[JUnitRunner])
class ServiceTest extends FunSuite {
  test("Service should rescue") {
    val e = new RuntimeException("yargs")
    val exceptionThrowingService = new Service[Int, Int] {
      def apply(request: Int) = {
        throw e
      }
    }

    assert(Try(Await.result(Service.rescue(exceptionThrowingService)(1), 1.second)) == Throw(e))
  }
}
