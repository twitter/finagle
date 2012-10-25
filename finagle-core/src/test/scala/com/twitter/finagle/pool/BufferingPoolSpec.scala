package com.twitter.finagle.pool

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.finagle._
import com.twitter.util.Future

class BufferingPoolSpec extends SpecificationWithJUnit with Mockito {
  val underlying = mock[ServiceFactory[Int, Int]]
  val service = mock[Service[Int, Int]]
  service.isAvailable returns true
  underlying(any) returns Future.value(service)
  val N = 10
  val pool = new BufferingPool(underlying, N)

  "BufferingPool" should {
    "buffer exactly N items" in {
      val n2 = for (_ <- 0 until N*2) yield pool()()
      there was no(service).release()
      there were (N*2).times(underlying).apply(any)
      for (s <- n2 take N)
        s.release()
      there was no(service).release()
      val n1 = for (_ <- 0 until N) yield pool()()
      there were (N*2).times(underlying).apply(any)
      for (s <- n1)
        s.release()
      there was no(service).release()
      for (s <- n2 drop N)
        s.release()
      there were N.times(service).release()
    }

    "drain services on close" in {
      val ns = for (_ <- 0 until N) yield pool()()
      there was no(service).release()
      for (s <- ns take (N-1)) s.release()
      pool.close()
      there were (N-1).times(service).release()
      ns(N-1).release()
      there were N.times(service).release()

      // Bypass buffer after drained.
      val s = pool()()
      there were (N+1).times(underlying).apply(any)
      s.release()
      there were (N+1).times(service).release()
    }

    "give back unhealthy services immediately" in {
      val unhealthy = mock[Service[Int, Int]]
      unhealthy.isAvailable returns false
      underlying(any) returns Future.value(unhealthy)
      val s1 = pool()()
      s1.isAvailable must beFalse
      s1.release()
      there was one(unhealthy).release()
    }

    "skip unhealthy services" in {
      val failing = mock[Service[Int, Int]]
      failing.isAvailable returns true
      underlying(any) returns Future.value(failing)
      pool()().release()
      there was no(failing).release()
      failing.isAvailable returns false
      pool()()
      there was one(failing).release()
    }
  }
}


