package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.service.ConstantService
import com.twitter.finagle.service.FailedService
import com.twitter.finagle.service.NilService
import com.twitter.util._
import org.mockito.Matchers._
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

object ServiceTest {

  class TestServiceFactory extends ServiceFactory[Int, Int] {
    def apply(conn: ClientConnection): Future[Service[Int, Int]] =
      Future.value(new ConstantService[Int, Int](Future.value(2)))

    def close(deadline: Time): Future[Unit] = Future.Done

    def status: Status = Status.Open
  }

  def await[A](fa: Future[A], timeout: Duration = 5.seconds): A =
    Await.result(fa, timeout)
}

class ServiceTest extends AnyFunSuite with MockitoSugar {
  import ServiceTest._

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

  test("Service.toString") {
    val constSvc = new ConstantService[Int, Int](Future.value(2))
    assert(
      constSvc.toString == "com.twitter.finagle.service.ConstantService(ConstFuture(Return(2)))"
    )

    val constSvcFactory = ServiceFactory.const(constSvc)
    assert(
      constSvcFactory.toString == "com.twitter.finagle.service.ConstantService(ConstFuture(Return(2)))"
    )

    val failedSvc = new FailedService(new Exception("test"))
    assert(
      failedSvc.toString == "com.twitter.finagle.service.FailedService(java.lang.Exception: test)"
    )

    assert(NilService.toString == "com.twitter.finagle.service.NilService$")

    val mkSvc = Service.mk[Int, Int] { x: Int => Future.value(x + 1) }
    assert(mkSvc.toString == "com.twitter.finagle.Service$$anon$2")

    val proxied = new ServiceProxy(constSvc) {}
    assert(
      proxied.toString == "com.twitter.finagle.service.ConstantService(ConstFuture(Return(2)))"
    )

    val proxiedWithToString = new ServiceProxy(constSvc) {
      override def toString: String = "ProxiedService"
    }
    assert(proxiedWithToString.toString == "ProxiedService")

    val svcFactory = new TestServiceFactory
    assert(svcFactory.toString == "com.twitter.finagle.ServiceTest$TestServiceFactory")

    val svcFactoryWithToString = new ServiceFactory[Int, Int] {
      def apply(conn: ClientConnection): Future[Service[Int, Int]] = Future.value(constSvc)
      def close(deadline: Time): Future[Unit] = Future.Done
      override def toString: String = "ServiceFactory"
      def status: Status = constSvc.status
    }
    assert(svcFactoryWithToString.toString == "ServiceFactory")
  }

  test(
    "ServiceFactory.const should resolve immediately to the given service" +
      "resolve immediately to the given service"
  ) {
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

  test("ServiceFactory.const propagate the initial service status") {
    val service = mock[Service[String, String]]
    when(service.status) thenReturn Status.Open
    when(service("ok")) thenReturn Future.value("ko")

    val factory = ServiceFactory.const(service)
    val newService = factory.toService

    assert(Await.result(newService("ok"), 2.seconds) == "ko")
    assert(newService.status == Status.Open)
    assert(factory.status == Status.Open)

    when(service.close(any)) thenReturn Future.Done
    when(service.status) thenReturn Status.Closed
    service.close()
    assert(newService.status == Status.Closed)
    assert(factory.status == Status.Closed)
  }

  test("ServiceFactory.flatMap should release underlying service on failure") {
    val exc = new Exception
    val service = mock[Service[String, String]]
    when(service.close(any)) thenReturn Future.Done
    val factory = new ServiceFactory[String, String] {
      def apply(conn: ClientConnection) = Future.value(service)
      def close(deadline: Time) = Future.Done
      def status: Status = service.status
    }

    verify(service, times(0)).close(any)

    var didRun = false
    val f2 = factory flatMap { _: Any =>
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
      def apply(conn: ClientConnection) =
        Future.value(new Service[Unit, Unit] {
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

  test("FactoryToService closes underlying service after request, does not close factory")(new Ctx {
    val service = new FactoryToService(underlyingFactory)
    Await.result(service(()))

    assert(serviceCloseCalled)
    assert(!factoryCloseCalled)
  })

  test("FactoryToService delegates status / close to underlying factory")(new Ctx {
    val service = new FactoryToService(underlyingFactory)
    service.status
    service.close()

    assert(statusCalled)
    assert(factoryCloseCalled)
  })

  test("FactoryToService module delegates isAvailable / close to underlying factory")(new Ctx {
    val stack =
      FactoryToService.module.toStack(Stack.leaf(Stack.Role("role"), underlyingFactory))

    val factory = stack.make(Stack.Params.empty + FactoryToService.Enabled(true))

    factory.status
    factory.close()

    assert(statusCalled)
    assert(factoryCloseCalled)
  })

  test(
    "FactoryToService around module closes underlying service after request, does not close underlying factory"
  )(new Ctx {
    val stack =
      FactoryToService.module.toStack(Stack.leaf(Stack.Role("role"), underlyingFactory))

    val factory = stack.make(Stack.Params.empty + FactoryToService.Enabled(true))

    val service = new FactoryToService(factory)
    Await.result(service(()))

    assert(serviceCloseCalled)
    assert(!factoryCloseCalled)
  })

  test("FactoryToService around module delegates isAvailable / close to underlying factory")(
    new Ctx {
      val stack =
        FactoryToService.module.toStack(Stack.leaf(Stack.Role("role"), underlyingFactory))

      val factory = stack.make(Stack.Params.empty + FactoryToService.Enabled(true))

      val service = new FactoryToService(factory)
      service.status
      service.close()

      assert(statusCalled)
      assert(factoryCloseCalled)
    }
  )

  test("pending: apply") {
    val ok = Service.const(Future.value("ok"))
    val boo = Service.const(Future.exception(new Exception("boo")))

    assert(await(Service.pending(Future.value(ok))(1)) == "ok")
    intercept[Exception] { await(Service.pending(Future.value(boo))(1)) }
  }

  test("pending: close") {
    var closeCalled: Boolean = false
    val underlying = new Service[Int, Int] {
      def apply(req: Int): Future[Int] = Future.value(0)
      override def close(deadline: Time): Future[Unit] = {
        closeCalled = true
        Future.Done
      }
    }

    val promise = new Promise[Service[Int, Int]]
    val svc = Service.pending(promise)
    assert(svc.status == Status.Busy)

    val rep = svc(1)
    val closed = svc.close()
    assert(closed.isDefined)
    assert(rep.isDefined)
    assert(svc.status == Status.Closed)

    promise.setValue(underlying)
    assert(closeCalled)
  }
}
