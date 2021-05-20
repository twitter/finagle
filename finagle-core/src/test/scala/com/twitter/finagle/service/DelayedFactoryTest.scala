package com.twitter.finagle.service

import com.twitter.finagle.{Status, ClientConnection, Service, ServiceFactory, Failure}
import com.twitter.util._
import org.scalatest.funsuite.AnyFunSuite

class DelayedFactoryTest extends AnyFunSuite {
  trait DelayedHelper {
    val service = Service.mk[Int, Int] { int: Int => Future.value(int) }
    val future = Promise[ServiceFactory[Int, Int]]()
    val failed = new Exception("failed")

    def underlying: ServiceFactory[Int, Int]
    def completeFuture(): Unit = {
      future.setValue(underlying)
    }

    def throwFuture(): Unit = {
      future.setException(failed)
    }
    def factory: ServiceFactory[Int, Int]
  }

  trait BareDelayedHelper extends DelayedHelper {
    val underlying = ServiceFactory.const(service)
    val factory = new DelayedFactory(future)
  }

  trait SwapOnBareHelper extends DelayedHelper {
    val underlying = ServiceFactory.const(service)
    val factory = DelayedFactory.swapOnComplete(future)
  }

  trait ClosingDelayedHelper extends DelayedHelper {
    val underlying = new ServiceFactory[Int, Int] {
      override def apply(conn: ClientConnection): Future[Service[Int, Int]] = {
        Future.value(service)
      }

      override def close(deadline: Time): Future[Unit] = {
        stat = Status.Closed
        Future.Done
      }

      var stat = Status.Open: Status
      override def status: Status = stat
    }

    def factory: ServiceFactory[Int, Int]
  }

  trait DefaultClosingHelper extends ClosingDelayedHelper {
    val factory = new DelayedFactory(future)
  }

  trait SwapOnCloseHelper extends ClosingDelayedHelper {
    val factory = DelayedFactory.swapOnComplete(future)
  }

  def numWaitersCheckFactory(factory: ServiceFactory[Int, Int], num: Int): Unit = {
    factory.getClass.getDeclaredMethods.find(_.getName == "numWaiters").foreach { meth =>
      assert(meth.invoke(factory) == num)
    }
  }

  def testDelayedHelpers(helpers: Map[String, () => DelayedHelper]): Unit = {
    for ((name, helpFn) <- helpers) {
      test(
        "%s: buffered factories' proxy services should buffer until the factory is ready"
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        numWaitersCheckFactory(factory, 0)
        val bufferF = factory()
        val num = 3
        assert(!bufferF.isDefined)
        numWaitersCheckFactory(factory, 1)
        completeFuture()
        assert(bufferF.isDefined)
        assert(Await.result(bufferF) == service)
      }

      test(
        ("%s: an incomplete buffered factory should satisfy closures with exceptions if they're " +
          "interrupted")
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        val bufferF = factory()
        assert(!bufferF.isDefined)
        val exc = new Exception("FAIL")
        bufferF.raise(exc)
        assert(bufferF.isDefined)
        val actual = intercept[Failure] {
          Await.result(bufferF)
        }
        assert(actual.getCause == exc)
      }

      test(
        "%s: an incomplete buffered factory should detach closures if they're interrupted"
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        val bufferF = factory()
        assert(!bufferF.isDefined)

        numWaitersCheckFactory(factory, 1)
        val exc = new Exception("FAIL")
        bufferF.raise(exc)
        assert(bufferF.isDefined)
        numWaitersCheckFactory(factory, 0)
        completeFuture()
        assert(factory.isAvailable)
      }

      test(
        "%s: an incomplete buffered factory should be OK with exceptions if they're interrupted"
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        val bufferF = factory()
        assert(!bufferF.isDefined)
        val exc = new Exception("FAIL")
        bufferF.raise(exc)
        assert(bufferF.isDefined)
        completeFuture()
        assert(factory.isAvailable)
      }

      test(
        "%s: a buffered factory that's completed with an exception should finish with an exception"
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        val bufferF = factory()
        assert(!bufferF.isDefined)
        throwFuture()
        assert(bufferF.isDefined)
        val actual = intercept[Exception] {
          Await.result(bufferF)
        }
        assert(actual == failed)
      }

      test(
        "%s: a factory that's completed with an exception should finish with an exception"
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        throwFuture()
        val bufferF = factory()
        assert(bufferF.isDefined)
        val actual = intercept[Exception] {
          Await.result(bufferF)
        }
        assert(actual == failed)
      }
    }
  }

  def testClosingDelayedHelpers(helpers: Map[String, () => ClosingDelayedHelper]): Unit = {
    for ((name, helpFn) <- helpers) {
      test(
        "%s: a closed buffered factory should close the underlying factory once it's ready"
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        val f = factory.close()
        assert(!factory.isAvailable)
        completeFuture()
        assert(f.isDefined)
        assert(!factory.isAvailable)
      }

      test(
        "%s: an unclosed buffered factory should get ready then get closed properly"
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        completeFuture()
        assert(factory.isAvailable)
        val f = factory.close()
        assert(f.isDefined)
        assert(!factory.isAvailable)
      }

      test("%s: a factory that's closed prematurely should still close".format(name)) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        val f = factory.close()
        assert(f.isDefined)
        assert(Await.result(f.liftToTry) == Return.Unit)
        assert(underlying.isAvailable)
        completeFuture()
        assert(!underlying.isAvailable)
      }

      test(
        "%s: a factory that's closed prematurely should close the underlying on satisfaction"
          .format(name)
      ) {
        val ctx = helpFn()
        import ctx._

        assert(!factory.isAvailable)
        val f = factory.close()
        assert(f.isDefined)
        assert(Await.result(f.liftToTry) == Return.Unit)
        completeFuture()
      }
    }
  }

  testDelayedHelpers(
    Map(
      "Normal" -> (() => new BareDelayedHelper {}),
      "Swapping" -> (() => new SwapOnBareHelper {})
    )
  )
  testClosingDelayedHelpers(
    Map(
      "Normal" -> (() => new DefaultClosingHelper {}),
      "Swapping" -> (() => new SwapOnCloseHelper {})
    )
  )

}
