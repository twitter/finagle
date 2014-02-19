package com.twitter.finagle.service

import com.twitter.finagle.{CancelledConnectionException, ClientConnection, Service, ServiceFactory}
import com.twitter.util.{Await, Future, Promise, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

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
    val failed = new Exception("failed")
    def throwFuture() {
      future.setException(failed)
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
      assert(factory.numWaiters === 0)
      val bufferF = factory()
      val num = 3
      assert(!bufferF.isDefined)
      assert(factory.numWaiters === 1)
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
      completeFuture()
      assert(f.isDefined)
      assert(!factory.isAvailable)
    }
  }

  test("an unclosed buffered factory should get ready then get closed properly") {
    new ClosingDelayedHelper {
      assert(!factory.isAvailable)
      completeFuture()
      assert(factory.isAvailable)
      val f = factory.close()
      assert(f.isDefined)
      assert(!factory.isAvailable)
    }
  }

  test("a factory that's closed prematurely should still close") {
    new ClosingDelayedHelper {
      assert(!factory.isAvailable)
      val f = factory.close()
      assert(f.isDefined)
      assert(Await.result(f) === ())
      assert(underlying.isAvailable)
      completeFuture()
      assert(!underlying.isAvailable)
    }
  }

  test("a factory that's closed prematurely should close the underlying on satisfaction") {
    new ClosingDelayedHelper {
      assert(!factory.isAvailable)
      val f = factory.close()
      assert(f.isDefined)
      assert(Await.result(f) === ())
      completeFuture()
    }
  }

  test("an incomplete buffered factory should satisfy closures with exceptions if they're interrupted") {
    new DelayedHelper {
      assert(!factory.isAvailable)
      val bufferF = factory()
      assert(!bufferF.isDefined)
      val exc = new Exception("FAIL")
      bufferF.raise(exc)
      assert(bufferF.isDefined)
      val actual = intercept[CancelledConnectionException] {
        Await.result(bufferF)
      }
      assert(actual.getCause === exc)
    }
  }

  test("an incomplete buffered factory should detach closures if they're interrupted") {
    new DelayedHelper {
      assert(!factory.isAvailable)
      val bufferF = factory()
      assert(!bufferF.isDefined)
      assert(factory.numWaiters === 1)
      val exc = new Exception("FAIL")
      bufferF.raise(exc)
      assert(bufferF.isDefined)
      assert(factory.numWaiters === 0)
      completeFuture()
      assert(factory.isAvailable)
    }
  }

  test("an incomplete buffered factory should be OK with exceptions if they're interrupted") {
    new DelayedHelper {
      assert(!factory.isAvailable)
      val bufferF = factory()
      assert(!bufferF.isDefined)
      val exc = new Exception("FAIL")
      bufferF.raise(exc)
      assert(bufferF.isDefined)
      completeFuture()
      assert(factory.isAvailable)
    }
  }

  test("a buffered factory that's completed with an exception should finish with an exception") {
    new DelayedHelper {
      assert(!factory.isAvailable)
      val bufferF = factory()
      assert(!bufferF.isDefined)
      throwFuture()
      assert(bufferF.isDefined)
      val actual = intercept[Exception] {
        Await.result(bufferF)
      }
      assert(actual === failed)
    }
  }

  test("a factory that's completed with an exception should finish with an exception") {
    new DelayedHelper {
      assert(!factory.isAvailable)
      throwFuture()
      val bufferF = factory()
      assert(bufferF.isDefined)
      val actual = intercept[Exception] {
        Await.result(bufferF)
      }
      assert(actual === failed)
    }
  }
}
