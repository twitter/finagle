package com.twitter.finagle.channel

import java.util.concurrent.TimeUnit

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import org.jboss.netty.util.{Timer, Timeout, TimerTask}
import org.jboss.netty.channel.{MessageEvent, Channels}

import com.twitter.util.{Future, Promise, Return, Throw}

import com.twitter.conversions.time._

object TimeoutBrokerSpec extends Specification with Mockito {
  "TimeoutBroker" should {
    class FakeBroker(reply: Future[AnyRef]) extends Broker {
      def apply(request: AnyRef) = reply
    }

    val timer = mock[Timer]
    val timeout = mock[Timeout]
    val replyFuture = spy(new Promise[AnyRef])
    val b = new FakeBroker(replyFuture)
    val e = mock[MessageEvent]
    var timerTask: TimerTask = null

    timer.newTimeout(
      any[TimerTask], Matchers.eq(100L),
      Matchers.eq(TimeUnit.MILLISECONDS)) answers {
        case Array(task: TimerTask, _, _) =>
          timerTask = task
          timeout
      }

    val tob = new TimeoutBroker(timer, b, 100.milliseconds)

    val f = tob(e)
    timerTask must notBeNull

    "time out after specfied time" in {
      // Emulate the actual timeout.
      timerTask.run(timeout)

      f.isDefined must beTrue
      f.isReturn must beFalse
      f() must throwA(new TimedoutRequestException)
    }

    "cancel timeout if the request completes in time" in {
      // Emulate succesfull completion.
      replyFuture() = Return("yay")
      there was one(timeout).cancel()
      timeout.isCancelled() returns true

      // Timeout fires.
      timerTask.run(timeout)
    }

    "cancel timeout if the request fails in time" in {
      // Emulate failed completion.
      val exc = new Exception
      replyFuture() = Throw(exc)
      there was one(timeout).cancel()
      timeout.isCancelled() returns true

      // Timeout fires.
      timerTask.run(timeout)
    }
  }
}
