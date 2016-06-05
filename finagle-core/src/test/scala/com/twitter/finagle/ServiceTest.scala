package com.twitter.finagle

import com.twitter.util._
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ServiceTest extends FunSuite with MockitoSugar {

  test("ServiceProxy should proxy all requests") {
    val service = mock[Service[String, String]]
    when(service.close(any)) thenReturn Future.Done
    when(service.status) thenReturn Status.Closed

    val proxied = new ServiceProxy(service) {}

    when(service.apply(any[String])) thenAnswer {
      new Answer[Future[String]] {
        override def answer(invocation: InvocationOnMock) = {
          if (proxied.status == Status.Open) service("ok")
          else Future("service is not available")
        }
      }
    }

    verify(service, times(0)).close(any)
    verify(service, times(0)).status
    verify(service, times(0))(any[String])

    proxied.close(Time.now)
    verify(service).close(any)
    assert(proxied.status == Status.Closed)
    verify(service).status

    assert(Await.result(proxied("ok")) == "service is not available")
    verify(service)("ok")
  }

  test("Service.rescue should wrap NonFatal exceptions in a failed Future") {
    val exc = new IllegalArgumentException
    val service = Service.mk[String, String] { _ => throw exc }
    val rescuedService = Service.rescue(service)

    val result = Await.result(rescuedService("ok").liftToTry)
    assert(result.throwable == exc)

    val fatalExc = new InterruptedException
    val service2 = Service.mk[String, String] { _ => throw fatalExc }
    val rescuedService2 = Service.rescue(service2)

    intercept[InterruptedException] {
      rescuedService2("fatal")
    }
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

    assert(proxied("ok").poll == Some(Return("ko")))
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
    assert(f2().poll == Some(Throw(exc)))
    assert(didRun)
    verify(service).close(any)
  }

  trait Ctx {
    var serviceCloseCalled = false
    var factoryCloseCalled = false
    var statusCalled = false

    val underlyingFactory = new ServiceFactory[Unit, Unit] {
      def apply(conn: ClientConnection) = Future.value(new Service[Unit, Unit] {
        def apply(request: Unit): Future[Unit] = Future.Unit
        override def close(deadline: Time) = {
          serviceCloseCalled = true
          Future.Done
        }
      })
      override def close(deadline: Time) = {
        factoryCloseCalled = true
        Future.Done
      }
      override def status: Status = {
        statusCalled = true
        Status.Open
      }
    }
  }

  test("FactoryToService closes underlying service after request, does not close factory") (new Ctx {
    val service = new FactoryToService(underlyingFactory)
    Await.result(service(Unit))

    assert(serviceCloseCalled)
    assert(!factoryCloseCalled)
  })

  test("FactoryToService delegates status / close to underlying factory") (new Ctx {
    val service = new FactoryToService(underlyingFactory)
    service.status
    service.close()

    assert(statusCalled)
    assert(factoryCloseCalled)
  })

  test("FactoryToService module delegates isAvailable / close to underlying factory") (new Ctx {
    val stack =
      FactoryToService.module.toStack(
        Stack.Leaf(Stack.Role("role"), underlyingFactory))

    val factory = stack.make(Stack.Params.empty + FactoryToService.Enabled(true))

    factory.status
    factory.close()

    assert(statusCalled)
    assert(factoryCloseCalled)
  })

  test("FactoryToService around module closes underlying service after request, does not close underlying factory") (new Ctx {
    val stack =
      FactoryToService.module.toStack(
        Stack.Leaf(Stack.Role("role"), underlyingFactory))

    val factory = stack.make(Stack.Params.empty + FactoryToService.Enabled(true))

    val service = new FactoryToService(factory)
    Await.result(service(Unit))

    assert(serviceCloseCalled)
    assert(!factoryCloseCalled)
  })

  test("FactoryToService around module delegates isAvailable / close to underlying factory") (new Ctx {
    val stack =
      FactoryToService.module.toStack(
        Stack.Leaf(Stack.Role("role"), underlyingFactory))

    val factory = stack.make(Stack.Params.empty + FactoryToService.Enabled(true))

    val service = new FactoryToService(factory)
    service.status
    service.close()

    assert(statusCalled)
    assert(factoryCloseCalled)
  })
}
