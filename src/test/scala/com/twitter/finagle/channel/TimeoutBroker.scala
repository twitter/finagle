package com.twitter.finagle.channel

import java.util.concurrent.TimeUnit

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import org.jboss.netty.util.{Timer, Timeout, TimerTask}
import org.jboss.netty.channel.{MessageEvent, Channels}

object TimeoutBrokerSpec extends Specification with Mockito {
  "TimeoutBroker" should {
    val timer = mock[Timer]
    val timeout = mock[Timeout]
    val b = mock[Broker]
    val e = mock[MessageEvent]
    var timerTask: TimerTask = null

    timer.newTimeout(
      any[TimerTask], Matchers.eq(100L),
      Matchers.eq(TimeUnit.MILLISECONDS)) answers {
        case Array(task: TimerTask, _, _) =>
          timerTask = task
          timeout
      }

    val f = spy(new ReplyFuture)
    b.dispatch(e) returns f

    val tob = new TimeoutBroker(timer, b, 100, TimeUnit.MILLISECONDS)

    tob.dispatch(e)
    there was one(b).dispatch(e)
    f.isDone must beFalse
    timerTask must notBeNull

    "time out after specfied time" in {
      // Emulate the actual timeout.
      timerTask.run(timeout)

      f.isDone must beTrue
      f.isSuccess must beFalse
      f.getCause must haveClass[TimedoutRequestException]
    }

    "cancel timeout if the request completes in time" in {
      // Emulate succesfull completion.
      f.setSuccess()
      there was one(timeout).cancel()
      timeout.isCancelled() returns true

      // Timeout fires.
      timerTask.run(timeout)
      there was no(f).setFailure(Matchers.any[Throwable])
    }

    "cancel timeout if the request fails in time" in {
      // Emulate failed completion.
      val exc = new Exception
      f.setFailure(exc)
      there was one(timeout).cancel()
      timeout.isCancelled() returns true

      // Timeout fires.
      timerTask.run(timeout)
      there was one(f).setFailure(exc)
    }
  }
}
