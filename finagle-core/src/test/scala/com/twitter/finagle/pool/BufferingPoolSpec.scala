package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.util.{Await, Future, Time}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class BufferingPoolSpec extends SpecificationWithJUnit with Mockito {
  val underlying = mock[ServiceFactory[Int, Int]]
  underlying.close(any[Time]) returns Future.Done
  val service = mock[Service[Int, Int]]
  service.close(any[Time]) returns Future.Done
  service.isAvailable returns true
  underlying(any) returns Future.value(service)
  val N = 10
  val pool = new BufferingPool(underlying, N)

  "BufferingPool" should {
    "buffer exactly N items" in {
      val n2 = for (_ <- 0 until N*2) yield Await.result(pool())
      there was no(service).close(any[Time])
      there were (N*2).times(underlying).apply(any)
      for (s <- n2 take N)
        s.close()
      there was no(service).close(any[Time])
      val n1 = for (_ <- 0 until N) yield Await.result(pool())
      there were (N*2).times(underlying).apply(any)
      for (s <- n1)
        s.close()
      there was no(service).close(any[Time])
      for (s <- n2 drop N)
        s.close()
      there were N.times(service).close(any[Time])
    }

    "drain services on close" in {
      val ns = for (_ <- 0 until N) yield Await.result(pool())
      there was no(service).close(any[Time])
      for (s <- ns take (N-1)) s.close()
      pool.close()
      there were (N-1).times(service).close(any[Time])
      ns(N-1).close()
      there were N.times(service).close(any[Time])

      // Bypass buffer after drained.
      val s = Await.result(pool())
      there were (N+1).times(underlying).apply(any)
      s.close()
      there were (N+1).times(service).close(any[Time])
    }

    "give back unhealthy services immediately" in {
      val unhealthy = mock[Service[Int, Int]]
      unhealthy.close(any[Time]) returns Future.Done
      unhealthy.isAvailable returns false
      underlying(any) returns Future.value(unhealthy)
      val s1 = Await.result(pool())
      s1.isAvailable must beFalse
      s1.close()
      there was one(unhealthy).close(any[Time])
    }

    "skip unhealthy services" in {
      val failing = mock[Service[Int, Int]]
      failing.close(any[Time]) returns Future.Done
      failing.isAvailable returns true
      underlying(any) returns Future.value(failing)
      Await.result(pool()).close()
      there was no(failing).close(any[Time])
      failing.isAvailable returns false
      Await.result(pool())
      there was one(failing).close(any[Time])
    }
  }
}


