package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, FuturePool}
import com.twitter.finagle.util.DefaultTimer.Implicit
import java.util.concurrent.Executors
import org.scalatest.FunSuite

class OffloadFilterTest extends FunSuite {
  test("client") {
    val executor = Executors.newFixedThreadPool(1)
    try {
      val next = new Service[Unit, Unit] {
        def apply(request: Unit): Future[Unit] = Future.sleep(200.milliseconds).unit
      }
      val s = new OffloadFilter.Client[Unit, Unit](FuturePool(executor)).andThen(next)
      val caller = Thread.currentThread()

      assert(Await.result(s().map(_ => Thread.currentThread()), 5.seconds) != caller)
    } finally {
      executor.shutdownNow()
    }
  }

  test("server") {
    val executor = Executors.newFixedThreadPool(1)
    try {
      val next = new Service[Unit, Thread] {
        def apply(request: Unit): Future[Thread] = Future.value(Thread.currentThread())
      }
      val s = new OffloadFilter.Server[Unit, Thread](FuturePool(executor)).andThen(next)
      val caller = Thread.currentThread()
      assert(Await.result(s(), 5.seconds) != caller)

    } finally {
      executor.shutdownNow()
    }
  }
}
