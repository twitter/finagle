package com.twitter.finagle.service

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Future, Promise, Return}

import com.twitter.finagle.Service
import com.twitter.finagle.util.DrainablePool

object PoolingServiceSpec extends Specification with Mockito {
  "PoolingService" should {
    val pool = mock[DrainablePool[Service[Any, Any]]]
    val service = mock[Service[Any, Any]]
    pool.reserve() returns Future.value(service)
    val promise = new Promise[Int]
    service(123) returns promise

    "give (a) service back to the pool on success" in {
      val poolingService = new PoolingService[Any, Any, Service[Any, Any]](pool)

      there was no(pool).reserve()
      val f = poolingService(123)
      there was one(pool).reserve()
      f.isDefined must beFalse
      there was no(pool).release(service)

      promise() = Return(321)
      f.isDefined must beTrue
      f() must be_==(321)

      there was one(pool).release(service)
    }
  }
}
