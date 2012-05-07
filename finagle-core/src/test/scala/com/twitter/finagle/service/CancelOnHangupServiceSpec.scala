package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.util.{Future, Promise, Return}
import com.twitter.finagle.{Service, ServiceFactory}

class CancelOnHangupServiceSpec extends SpecificationWithJUnit with Mockito {
  "CancelOnHangupService" should {
    val underlying = mock[Service[Unit, Unit]]
    val service = new CancelOnHangupService(underlying)

    val p0, p1 = new Promise[Unit]
    underlying(()) returns p0
    val f0 = service(())
    underlying(()) returns p1
    val f1 = service(())
    f0.isDefined must beFalse
    f1.isDefined must beFalse
    f0.isCancelled must beFalse
    f1.isCancelled must beFalse

    "cancel outstanding responses" in {
      service.release()
      f0.isCancelled must beTrue
      f1.isCancelled must beTrue
      there was one(underlying).release()
    }

    "not cancel completed responses" in {
      p0() = Return(())
      f0.poll must beSome(Return(()))
      service.release()
      f0.isCancelled must beFalse
      f1.isCancelled must beTrue
      there was one(underlying).release()
    }
  }
}
