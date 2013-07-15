package com.twitter.finagle.service

import com.twitter.finagle.{Service, WriteException}
import com.twitter.util.{Await, Future, Promise}
import org.mockito.Matchers
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class CloseOnReleaseServiceSpec extends SpecificationWithJUnit with Mockito {
  "CloseOnReleaseService" should {
    val service = mock[Service[Any, Any]]
    service.close(any) returns Future.Done
    val promise = new Promise[Any]
    service(Matchers.any) returns promise
    val wrapper = new CloseOnReleaseService(service)

    "only call release on the underlying service once" in {
      wrapper.close()
      there was one(service).close(any)

      wrapper.close()
      there was one(service).close(any)

      service.isAvailable must beFalse
    }

    "throw a write exception if we attempt to use a released service" in {
      wrapper.close()

      Await.result(wrapper(132)) must throwA[WriteException]
    }
  }
}

