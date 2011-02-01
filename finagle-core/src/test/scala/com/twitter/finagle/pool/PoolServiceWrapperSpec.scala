package com.twitter.finagle.pool

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import com.twitter.util.{Promise, Return}
import com.twitter.finagle.Service

object PoolServiceWrapperSpec extends Specification with Mockito {
  "PoolServiceWrapper" should {
    class DelegatingWrapper[Req, Rep](service: Service[Req, Rep])
      extends PoolServiceWrapper[Req, Rep](service)
    {
      def doRelease() { didRelease() }
      def didRelease() {}
    }

    val service = mock[Service[Any, Any]]
    val promise = new Promise[Any]
    service(Matchers.any) returns promise
    val wrapper = spy(new DelegatingWrapper[Any, Any](service))

    "call doRelease immediately when no requests have been made" in {
      there was no(wrapper).didRelease()
      wrapper.release()
      there was one(wrapper).didRelease()
    }

    "call doRelease after pending request finishes" in {
      val f = wrapper(123)
      f.isDefined must beFalse
      there was one(service)(123)

      wrapper.release()
      there was no(wrapper).didRelease()

      promise() = Return(123)
      there was one(wrapper).didRelease()
      f.isDefined must beTrue
      f() must be_==(123)
    }
  }
}
