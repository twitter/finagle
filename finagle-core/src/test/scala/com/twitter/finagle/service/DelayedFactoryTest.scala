package com.twitter.finagle.service

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory}
import com.twitter.util.{Await, Future, Promise, Time}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class DelayedFactoryTest extends FunSuite {
  trait DelayedHelper {
    val service = Service.mk[Int, Int] { int: Int =>
      Future.value(int)
    }
    val underlying = ServiceFactory.const(service)
    val future = Promise[ServiceFactory[Int, Int]]()
    def completeFuture() {
      future.setValue(underlying)
    }
    val factory = new DelayedFactory(future)
  }

  trait ClosingDelayedHelper {
    val service = new CloseOnReleaseService(Service.mk[Int, Int] { int: Int =>
      Future.value(int)
    })
    val underlying = new ServiceFactory[Int, Int] {
      override def apply(conn: ClientConnection): Future[Service[Int, Int]] = {
        Future.value(service)
      }

      override def close(deadline: Time): Future[Unit] = {
        avail = false
        Future.Done
      }

      var avail = true
      override def isAvailable: Boolean = avail
    }
    val future = Promise[ServiceFactory[Int, Int]]()
    def completeFuture() {
      future.setValue(underlying)
    }
    val factory = new DelayedFactory(future)
  }

  test("buffered factories' proxy services should buffer until the factory is ready") {
    new DelayedHelper {
      val bufferF = factory()
      val num = 3
      assert(!bufferF.isDefined)
      completeFuture()
      assert(bufferF.isDefined)
      assert(Await.result(bufferF) === service)
    }
  }

  test("a closed buffered factory should close the underlying factory once it's ready") {
    new ClosingDelayedHelper {
      assert(!factory.isAvailable)
      val f = factory.close()
      assert(!factory.isAvailable)
      assert(!f.isDone)
      completeFuture()
      Await.result(f)
      assert(!factory.isAvailable)
    }
  }

  test("an unclosed buffered factory should get ready then get closed properly") {
    new ClosingDelayedHelper {
      assert(!factory.isAvailable)
      completeFuture()
      assert(factory.isAvailable)
      Await.result(factory.close())
      assert(!factory.isAvailable)
    }
  }
}
