package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.mockito.Matchers

import com.twitter.util.Promise
import com.twitter.finagle.{Service, WriteException}

class CloseOnReleaseServiceSpec extends SpecificationWithJUnit with Mockito {
  "CloseOnReleaseService" should {
    val service = mock[Service[Any, Any]]
    val promise = new Promise[Any]
    service(Matchers.any) returns promise
    val wrapper = new CloseOnReleaseService(service)

    "only call release on the underlying service once" in {
      wrapper.release()
      there was one(service).release()

      wrapper.release()
      there was one(service).release()

      service.isAvailable must beFalse
    }

    "throw a write exception if we attempt to use a released service" in {
      wrapper.release()

      wrapper(132)() must throwA[WriteException]
    }
  }
}

