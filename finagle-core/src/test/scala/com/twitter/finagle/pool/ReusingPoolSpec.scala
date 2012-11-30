package com.twitter.finagle.pool

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.finagle._
import com.twitter.util.{Future, Promise, Return, Throw}

class ReusingPoolSpec extends SpecificationWithJUnit with Mockito {
  "ReusingPool" should {
    val underlying = mock[ServiceFactory[Int, Int]]
    underlying.isAvailable returns true
    val service = mock[Service[Int, Int]]
    service.isAvailable returns true
    val service2 = mock[Service[Int, Int]]
    service2.isAvailable returns true
    val underlyingP = new Promise[Service[Int, Int]]
    underlying(any) returns underlyingP
    val pool = new ReusingPool(underlying)

    "be available when underlying is" in {
      pool.isAvailable must be_==(true)
      underlying.isAvailable returns false
      pool.isAvailable must be_==(false)
      there were two(underlying).isAvailable
    }

    "first attempt establishes a connection, subsequent attempts don't" in {
      there was no(underlying)(any)
      val f = pool()
      f.poll must beNone
      there was one(underlying)(any)
      val g = pool()
      g.poll must beNone
      there was one(underlying)(any)
      f must be(g)
      underlyingP.setValue(service)
      f.poll must beSome(Return(service))
    }

    "reestablish connections when the service becomes unavailable, releasing dead service" in {
      underlyingP.setValue(service)
      pool().poll must beSome(Return(service))
      there was one(underlying)(any)
      service.isAvailable returns false
      underlying(any) returns Future.value(service2)
      there was no(service).release()
      there was no(service).isAvailable
      pool().poll must beSome(Return(service2))
      there was one(service).isAvailable
      there was one(service).release()
      there were two(underlying)(any)
      there was no(service2).release()
    }

    "return on failure" in {
      val exc = new Exception
      underlyingP.setException(exc)
      pool().poll must beSome(Throw(exc))
      there was one(underlying)(any)
      pool().poll must beSome(Throw(exc))
      there were two(underlying)(any)
    }
  }
}
