package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Throw, Try}
import com.twitter.conversions.time._

class ServiceSpec extends SpecificationWithJUnit {
  "Service" should {
    "rescue" in {
      val e = new RuntimeException("yargs")
      val exceptionThrowingService = new Service[Int, Int] {
        def apply(request: Int) = {
          throw e
          Future.value(request + 1)
        }
      }

      Try(Await.result(Service.rescue(exceptionThrowingService)(1), 1.second)) must be_==(Throw(e))
    }
  }
}
