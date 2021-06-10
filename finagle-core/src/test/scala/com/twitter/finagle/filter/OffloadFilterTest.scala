package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.context.Contexts
import com.twitter.util.{Await, Awaitable, Future, FuturePool, Promise}
import com.twitter.finagle.util.DefaultTimer.Implicit
import java.util.concurrent.{CountDownLatch, Executors}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class OffloadFilterTest extends AnyFunSuite with BeforeAndAfterAll {
  private class ExpectedException extends Exception("boom")

  // NOTE: some tests require that this be a single thread so that we can ensure that
  // the task queue is clear via executing a single task.
  private[this] val executor = Executors.newFixedThreadPool(1)

  override def afterAll(): Unit = {
    executor.shutdown()
  }

  // A helper that just ensures that our executor is clear by submitting a single task and awaiting
  // the completion of the task. This relies on `executor` being backed by a single thread.
  private[this] def awaitClearExecutor(): Unit = {
    val latch = new CountDownLatch(1)
    executor.submit(new Runnable {
      def run(): Unit = latch.countDown()
    })
    latch.await()
  }

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  test("client") {
    val next = Service.mk[Unit, Unit] { _ => Future.sleep(200.milliseconds).unit }
    val s = new OffloadFilter.Client[Unit, Unit](FuturePool(executor)).andThen(next)
    val caller = Thread.currentThread()

    assert(Await.result(s(()).map(_ => Thread.currentThread()), 5.seconds) != caller)
  }

  test("client with synchronous exception") {
    val ex = new ExpectedException
    val next = Service.mk[Unit, Unit] { _ => throw ex }
    val s = new OffloadFilter.Client[Unit, Unit](FuturePool(executor)).andThen(next)

    intercept[ExpectedException] {
      await(s(()))
    }
  }

  test("client with fatal synchronous exception") {
    val ex = new InterruptedException
    val next = Service.mk[Unit, Unit] { _ => throw ex }
    val s = new OffloadFilter.Client[Unit, Unit](FuturePool(executor)).andThen(next)

    intercept[InterruptedException] {
      await(s(()))
    }
  }

  test("client with asynchronous exception") {
    val ex = new ExpectedException
    val next = Service.mk[Unit, Unit] { _ => Future.exception(ex) }
    val s = new OffloadFilter.Client[Unit, Unit](FuturePool(executor)).andThen(next)

    intercept[ExpectedException] {
      await(s(()))
    }
  }

  test("server") {
    val next = Service.mk[Unit, Thread] { _ => Future.value(Thread.currentThread()) }
    val s = new OffloadFilter.Server[Unit, Thread](FuturePool(executor)).andThen(next)
    val caller = Thread.currentThread()
    assert(await(s(())) != caller)
  }

  test("server with synchronous service execution exceptions") {
    val ex = new ExpectedException
    val next = Service.mk[Unit, Thread] { _ => throw ex }
    val s = new OffloadFilter.Server[Unit, Thread](FuturePool(executor)).andThen(next)
    intercept[ExpectedException] {
      await(s(()))
    }
  }

  test("server with fatal synchronous service execution exceptions") {
    val ex = new InterruptedException
    val next = Service.mk[Unit, Thread] { _ => throw ex }
    val s = new OffloadFilter.Server[Unit, Thread](FuturePool(executor)).andThen(next)
    val found = intercept[InterruptedException] {
      await(s(()))
    }
    assert(found eq ex)
  }

  test("server with asynchronous service execution exceptions") {
    val ex = new ExpectedException
    val next = Service.mk[Unit, Thread] { _ => Future.exception(ex) }
    val s = new OffloadFilter.Server[Unit, Thread](FuturePool(executor)).andThen(next)
    intercept[ExpectedException] {
      await(s(()))
    }
  }

  test("server interrupts during synchronous service execution") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)
    val p = Promise[Unit]()

    val next = Service.mk[Unit, Unit] { _ =>
      latch1.countDown()
      latch2.await()
      p.setInterruptHandler { case ex => p.setException(new Exception("chained boom", ex)) }
      p
    }
    val s = new OffloadFilter.Server[Unit, Unit](FuturePool.interruptible(executor)).andThen(next)
    val f = s()
    val ex = new Exception("boom")
    latch1.await()
    f.raise(ex)
    latch2.countDown()

    val filterFuture = intercept[Exception] { Await.result(f, 5.seconds) }
    val servicePromise = intercept[Exception] { Await.result(p, 5.seconds) }
    assert(filterFuture.getMessage == "chained boom")
    assert(servicePromise.getMessage == "chained boom")
  }

  test("server interrupts for service Future interruption") {
    val p = Promise[Unit]()
    val next = Service.mk[Unit, Unit] { _ =>
      p.setInterruptHandler { case ex => p.setException(new Exception("chained boom", ex)) }
      p
    }
    val s = new OffloadFilter.Server[Unit, Unit](FuturePool.interruptible(executor)).andThen(next)
    val f = s()
    val ex = new Exception("boom")
    awaitClearExecutor() // make sure the work in the future pool is complete.
    f.raise(ex)

    val filterFuture = intercept[Exception] { Await.result(f, 5.seconds) }
    val servicePromise = intercept[Exception] { Await.result(p, 5.seconds) }
    assert(filterFuture.getMessage == "chained boom")
    assert(servicePromise.getMessage == "chained boom")
  }

  test("server propagates locals") {
    val key = Contexts.local.newKey[Int]()
    val next = Service.mk[Unit, Option[Int]] { _ => Future.value(Contexts.local.get(key)) }
    val s =
      new OffloadFilter.Server[Unit, Option[Int]](FuturePool.interruptible(executor)).andThen(next)
    assert(await(s()) == None)
    assert(await(Contexts.local.let(key, 4) { s() }) == Some(4))
  }
}
