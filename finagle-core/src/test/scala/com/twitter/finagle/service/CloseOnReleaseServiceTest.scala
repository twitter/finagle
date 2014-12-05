package com.twitter.finagle.service

import org.junit.runner.RunWith
import org.mockito.Mockito.{verify, when, times}
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import com.twitter.finagle.{WriteException, Service, Status}
import com.twitter.util.{Await, Promise, Future}

@RunWith(classOf[JUnitRunner])
class CloseOnReleaseServiceTest extends FunSuite with MockitoSugar {

  class Helper {
    val service = mock[Service[Any, Any]]
    when(service.close(any)) thenReturn Future.Done
    val promise = new Promise[Any]
    when(service(any)) thenReturn promise
    when(service.status) thenReturn Status.Open
    val wrapper = new CloseOnReleaseService(service)
  }

  test("only call release on the underlying service once") {
    val h = new Helper
    import h._
    
    assert(wrapper.isAvailable)
    verify(service, times(1)).status

    wrapper.close()
    verify(service, times(1)).close(any)
    wrapper.close()
    verify(service, times(1)).close(any)
    
    assert(!wrapper.isAvailable)
    verify(service, times(1)).status
  }

  test("throw a write exception if we attempt to use a released service") {
    val h = new Helper
    import h._

    wrapper.close()
    intercept[WriteException] {
      Await.result(wrapper(132))
    }
  }
}
