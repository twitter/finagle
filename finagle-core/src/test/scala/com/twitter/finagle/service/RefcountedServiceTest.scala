package com.twitter.finagle.service

import com.twitter.util._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.ArgumentMatchers._
import com.twitter.finagle.Service
import org.scalatest.funsuite.AnyFunSuite

class RefcountedServiceTest extends AnyFunSuite with MockitoSugar {

  class PoolServiceWrapperHelper {
    val service = mock[Service[Any, Any]]
    when(service.close(any)) thenReturn Future.Done
    val promise = new Promise[Any]
    when(service(ArgumentMatchers.any)) thenReturn promise
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
