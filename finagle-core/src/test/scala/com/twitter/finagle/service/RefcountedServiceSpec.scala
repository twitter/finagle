package com.twitter.finagle.service

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Promise, Return}
import org.mockito.Matchers
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class RefcountedServiceSpec extends SpecificationWithJUnit with Mockito {
  "PoolServiceWrapper" should {
    val service = mock[Service[Any, Any]]
    service.close(any) returns Future.Done
    val promise = new Promise[Any]
    service(Matchers.any) returns promise
    val wrapper = spy(new RefcountedService[Any, Any](service))

    "call release() immediately when no requests have been made" in {
      there was no(service).close(any)
      wrapper.close()
      there was one(service).close(any)
    }

    "call release() after pending request finishes" in {
      val f = wrapper(123)
      f.isDefined must beFalse
      there was one(service)(123)

      wrapper.close()
      there was no(service).close(any)

      promise() = Return(123)
      there was one(service).close(any)
      f.isDefined must beTrue
      Await.result(f) must be_==(123)
    }
  }
}

