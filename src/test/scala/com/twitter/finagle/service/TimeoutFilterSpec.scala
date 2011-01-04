package com.twitter.finagle.service

import java.util.concurrent.TimeUnit

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.ArgumentCaptor

import org.jboss.netty.util.{Timer, TimerTask, Timeout}

import com.twitter.util.{Future, Promise, Return, Throw}
import com.twitter.conversions.time._

import com.twitter.finagle.channel.TimedoutRequestException

object TimeoutFilterSpec extends Specification with Mockito {
  "TimeoutFilter" should {
    val timer = mock[Timer]
    val timeout = mock[Timeout]
    val duration = 50.milliseconds
    val (timeoutValue, timeoutUnit) = duration.inTimeUnit
    val request = mock[Object]
    val service = mock[Service[AnyRef, AnyRef]]
    val responseFuture = new Promise[AnyRef]
    val response = mock[Object]
    val taskCaptor = ArgumentCaptor.forClass(classOf[TimerTask])
    timer.newTimeout(taskCaptor.capture, ==(timeoutValue), ==(timeoutUnit)) returns timeout
    service(request) returns responseFuture
    val timeoutFilter = new TimeoutFilter[AnyRef, AnyRef](timer, duration)
    val future = timeoutFilter(request, service)

    "cancels the request when the service succeeds" in {
      responseFuture() = Return(response)
      there was one(timeout).cancel
      future() must be_==(response)
    }

    "times out a request that is not successful" in {
      // Time out:
      val task = taskCaptor.getValue()
      task.run(timeout)

      future.isDefined must beTrue
      future.isThrow must beTrue

      val Throw(exc) = future.within(0.seconds)
      exc must haveClass[TimedoutRequestException]
    }
  }
}
