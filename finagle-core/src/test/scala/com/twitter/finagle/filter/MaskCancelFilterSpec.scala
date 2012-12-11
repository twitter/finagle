package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.util.{Promise, Return}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class MaskCancelFilterSpec extends SpecificationWithJUnit with Mockito {
  "MaskCancelFilter" should {
    val service = mock[Service[Int, Int]]
    val filter = new MaskCancelFilter[Int, Int]

    val filtered = filter andThen service
    val p = new Promise[Int] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler { case exc => interrupted = Some(exc) }
    }
    service(1) returns p

    val f = filtered(1)
    there was one(service).apply(1)

    "mask interrupts" in {
      p.interrupted must beNone
      f.cancel()
      p.interrupted must beNone
    }

    "propagate results" in {
      f.poll must beNone
      p.setValue(123)
      p.poll must beSome(Return(123))
    }
  }
}
