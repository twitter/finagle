package com.twitter.finagle.service

import org.specs.Specification

import com.twitter.finagle.{Service, Filter}
import com.twitter.util.{Throw, Future}
import com.twitter.conversions.time._

object FilterSpec extends Specification {
  "filters" should {
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

    "compose" in {
      "when it's all chill" in {
        val filter = stringToInt andThen intToString

        val service = new Service[Int, Int] {
          def apply(request: Int) = Future(2 * request.intValue)
        }

        val result = (filter andThen service)(123)

        result.isReturn must beTrue
        result() must be_==(123 * 2)
      }

      "when synchronous exceptions are thrown" in {
        val e = new RuntimeException("yargs")
        val exceptionThrowingService = new Service[Int, Int] {
          def apply(request: Int) = {
            throw e
            Future.value(request + 1)
          }
        }

        "with simple composition" in {
          intToString.andThen(exceptionThrowingService)("1").get(1.second) must
            be_==(Throw(e))
        }

        "with transitive composition" in {
          stringToInt.andThen(intToString.andThen(exceptionThrowingService))(1).get(1.second) must
            be_==(Throw(e))
          (stringToInt.andThen(intToString)).andThen(exceptionThrowingService)(1).get(1.second) must
            be_==(Throw(e))
        }
      }
    }
  }
}
