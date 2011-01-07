package com.twitter.finagle.util

import java.util.concurrent.TimeUnit

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{ArgumentCaptor, Matchers}

import org.jboss.netty.util.{Timer, TimerTask, Timeout}

import com.twitter.util.{Throw, Future, Promise, Return}
import com.twitter.conversions.time._

object TimerSpec extends Specification with Mockito {
  "RichTimer" should {
    val timer = mock[Timer]
    val richTimer = new RichTimer(timer)
    val timeout = mock[Timeout]
    val taskCaptor = ArgumentCaptor.forClass(classOf[TimerTask])
    timer.newTimeout(
      taskCaptor.capture,
      Matchers.eq(10000L),
      Matchers.eq(TimeUnit.MILLISECONDS)) returns timeout

    var wasInvoked = false
    richTimer(10.seconds) { wasInvoked = true }
    there was one(timer).newTimeout(
      any[TimerTask],
      Matchers.eq(10000L),
      Matchers.eq(TimeUnit.MILLISECONDS))
    val timeoutTask = taskCaptor.getValue
    wasInvoked must beFalse

    "not execute the task until it has timed out" in {
      timeout.isCancelled returns false
      timeoutTask.run(timeout)
      wasInvoked must beTrue
    }

    "not execute the task if it has been cancelled" in {
      timeout.isCancelled returns true
      timeoutTask.run(timeout)
      wasInvoked must beFalse
    }
  }

  "TimerFuture" should {
    val timer = mock[Timer]
    val richTimer = new RichTimer(timer)
    val timeout = mock[Timeout]
    val taskCaptor = ArgumentCaptor.forClass(classOf[TimerTask])
    timer.newTimeout(
      taskCaptor.capture,
      Matchers.eq(10000L),
      Matchers.eq(TimeUnit.MILLISECONDS)) returns timeout
    val timerFuture = TimerFuture[Unit](timer, 10.seconds, Throw(new Exception))

    there was one(timer).newTimeout(
      any[TimerTask],
      Matchers.eq(10000L),
      Matchers.eq(TimeUnit.MILLISECONDS))

    val timeoutTask = taskCaptor.getValue

    timerFuture.isDefined must beFalse

    "be satisfied when the timer is" in {
      timeoutTask.run(timeout)
      timerFuture() must throwA(new Exception)
    }

    "compose with Future.select" in {
      val future = new Promise[Unit]
      val selected = Future.select(future, timerFuture)
      selected.isDefined must beFalse
      future() = Return(())
      selected() must be_==(())
    }

  }
}
