package com.twitter.finagle.service

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.twitter.util.TimeConversions._
import com.twitter.finagle.{Service, Filter}
import com.twitter.util.{Throw, Try, Await, Future}

@RunWith(classOf[JUnitRunner])
class FilterTest extends FunSuite {
  class FilterHelper {
    val stringToInt =
      new Filter[Int, Int, String, String] {
        def apply(request: Int, service: Service[String, String]) =
          service(request.toString) map (_.toInt)
      }

    val intToString =
      new Filter[String, String, Int, Int] {
        def apply(request: String, service: Service[Int, Int]) =
          service(request.toInt) map (_.toString)
      }
  }

  test("filters should compose when it's all chill") {
    val h = new FilterHelper
    import h._

    val filter = stringToInt andThen intToString

    val service = new Service[Int, Int] {
      def apply(request: Int) = Future(2 * request.intValue)
    }

    val result = (filter andThen service)(123)

    assert(Await.ready(result).poll.get.isReturn)
    assert(Await.result(result) == (123 * 2))
  }

  test("filters should compose when synchronous exceptions are thrown with simple composition") {
    val h = new FilterHelper
    import h._

    // set up
    val e = new RuntimeException("yargs")
    val exceptionThrowingService = new Service[Int, Int] {
      def apply(request: Int) = {
        throw e
      }
    }

    assert(Try(Await.result(intToString.andThen(exceptionThrowingService)("1"), 1.second)) == Throw(e))
  }

  test("filters should compose when synchronous exceptions are thrown with transitive composition") {
    val h = new FilterHelper
    import h._

    // set up
    val e = new RuntimeException("yargs")
    val exceptionThrowingService = new Service[Int, Int] {
      def apply(request: Int) = {
        throw e
      }
    }

    assert(Try(Await.result(stringToInt.andThen(
      intToString.andThen(exceptionThrowingService))(1), 1.second)) == Throw(e))
    assert(Try(Await.result(stringToInt.andThen(
      intToString).andThen(exceptionThrowingService)(1), 1.second)) == Throw(e))
  }

}
