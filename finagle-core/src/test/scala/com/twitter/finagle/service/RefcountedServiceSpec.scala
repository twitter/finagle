package com.twitter.finagle.service

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import com.twitter.util.{Promise, Return}
import com.twitter.finagle.Service

object RefcountedServiceSpec extends Specification with Mockito {
  "PoolServiceWrapper" should {
    val service = mock[Service[Any, Any]]
    val promise = new Promise[Any]
    service(Matchers.any) returns promise
    val wrapper = spy(new RefcountedService[Any, Any](service))

    "call release() immediately when no requests have been made" in {
      there was no(service).release()
      wrapper.release()
      there was one(service).release()
    }

    "call release() after pending request finishes" in {
      val f = wrapper(123)
      f.isDefined must beFalse
      there was one(service)(123)

      wrapper.release()
      there was no(service).release()

      promise() = Return(123)
      there was one(service).release()
      f.isDefined must beTrue
      f() must be_==(123)
    }
  }
}

