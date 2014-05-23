package com.twitter.finagle

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{times, verify, when}
import org.mockito.Matchers._
import com.twitter.util._
import scala.Some
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

@RunWith(classOf[JUnitRunner])
class ServiceTest extends FunSuite with MockitoSugar {

  test("ServiceProxy should proxy all requests") {
    val service = mock[Service[String, String]]
    when(service.close(any)) thenReturn Future.Done
    when(service.isAvailable) thenReturn false

    val proxied = new ServiceProxy(service) {}

    when(service.apply(any[String])) thenAnswer {
      new Answer[Future[String]] {
        override def answer(invocation: InvocationOnMock) = {
          if (proxied.isAvailable) service("ok")
          else Future("service is not available")
        }
      }
    }

    verify(service, times(0)).close(any)
    verify(service, times(0)).isAvailable
    verify(service, times(0))(any[String])

    proxied.close(Time.now)
    verify(service).close(any)
    assert(!proxied.isAvailable)
    verify(service).isAvailable

    assert(Await.result(proxied("ok")) === "service is not available")
    verify(service)("ok")
  }

  test("ServiceFactory.const should resolve immediately to the given service" +
    "resolve immediately to the given service") {
    val service = mock[Service[String, String]]
    when(service.close(any)) thenReturn Future.Done
    when(service("ok")) thenReturn Future.value("ko")
    val factory = ServiceFactory.const(service)

    val f: Future[Service[String, String]] = factory()
    assert(f.isDefined)
    val proxied = Await.result(f)

    assert(proxied("ok").poll === Some(Return("ko")))
    verify(service)("ok")
  }

  test("ServiceFactory.flatMap should release underlying service on failure") {
    val exc = new Exception
    val service = mock[Service[String, String]]
    when(service.close(any)) thenReturn Future.Done
    val factory = new ServiceFactory[String, String] {
      def apply(conn: ClientConnection) = Future.value(service)
      def close(deadline: Time) = Future.Done
    }

    verify(service, times(0)).close(any)

    var didRun = false
    val f2 = factory flatMap { _ =>
      didRun = true
      Future.exception(exc)
    }

    assert(!didRun)
    verify(service, times(0)).close(any)
    assert(f2().poll === Some(Throw(exc)))
    assert(didRun)
    verify(service).close(any)
  }

}
