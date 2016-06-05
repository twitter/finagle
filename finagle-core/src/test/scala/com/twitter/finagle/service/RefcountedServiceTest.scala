package com.twitter.finagle.service

import com.twitter.util._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{times, verify, when}
import org.mockito.{Matchers, Mockito}
import org.mockito.Matchers._
import com.twitter.finagle.Service

@RunWith(classOf[JUnitRunner])
class RefcountedServiceTest extends FunSuite with MockitoSugar {

  class PoolServiceWrapperHelper {
    val service = mock[Service[Any, Any]]
    when(service.close(any)) thenReturn Future.Done
    val promise = new Promise[Any]
    when(service(Matchers.any)) thenReturn promise
    val wrapper = Mockito.spy(new RefcountedService[Any, Any](service))
  }

  test("PoolServiceWrapper should call release() immediately when no requests have been made") {
    val h = new PoolServiceWrapperHelper
    import h._

    verify(service, times(0)).close(any)
    wrapper.close()
    verify(service).close(any)
  }

  test("PoolServiceWrapper should call release() after pending request finishes") {
    val h = new PoolServiceWrapperHelper
    import h._

    val f = wrapper(123)
    assert(!f.isDefined)
    verify(service)(123)

    wrapper.close()
    verify(service, times(0)).close(any)

    promise() = Return(123)
    verify(service).close(any)
    assert(f.isDefined)
    assert(Await.result(f) == 123)
  }
}
