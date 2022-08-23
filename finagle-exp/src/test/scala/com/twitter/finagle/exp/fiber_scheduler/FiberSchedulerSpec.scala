package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.conversions.DurationOps.richDurationFromInt
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import java.util.concurrent.TimeUnit
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import java.util.concurrent.{CountDownLatch => JCountDownLatch}

trait FiberSchedulerSpec extends AnyFreeSpec with Eventually {

  val debug = false
  val timeout = if (debug) Duration.Top else 5.seconds

  case class CountDownLatch(count: Int) {
    val underlying = new JCountDownLatch(count)
    def countDown(): Unit = underlying.countDown()
    def await() = {
      if (timeout == Duration.Top) underlying.await()
      else if (!underlying.await(timeout.inSeconds * 2, TimeUnit.SECONDS)) {
        fail("timeout while waiting for a CountDownLatch")
      }
    }
  }

  def await[T](f: Future[T]): T =
    Await.result(f, timeout)

  def awaitReady[T](f: Future[T]): Future[T] =
    Await.ready(f, timeout)

  def eventually(f: => Boolean): Unit =
    eventually(timeout(Span(timeout.inSeconds, Seconds)))(assert(f))
}
