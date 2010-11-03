package com.twitter.finagle.channel

import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._

import org.specs.Specification
import org.specs.mock.Mockito

import org.jboss.netty.util.{Timeout, Timer, TimerTask}
import org.jboss.netty.channel.MessageEvent

object TimeoutBrokerSpec extends Specification with Mockito {
  "TimeoutBroker" should {
    val broker = mock[Broker]
    val timeout = mock[Timeout]
    val event = mock[MessageEvent]
    val brokerReplyFuture = new ReplyFuture

    broker.dispatch(event) returns brokerReplyFuture

    val timer = new Timer {
      var theTask: Option[TimerTask] = None

      def newTimeout(task: TimerTask, duration: Long, unit: TimeUnit) = {
        if (theTask.isDefined)
          throw new Exception("run() called twice")

        theTask = Some(task)
        timeout
      }

      def stop() = {
        throw new Exception("stop() was called")
      }
    }

    val timeoutBroker = new TimeoutBroker(timer, broker, 10L, TimeUnit.MILLISECONDS)

    "set failure when run" in {
      val replyFuture = timeoutBroker.dispatch(event)
      timer.theTask must beSomething
      val Some(theTask) = timer.theTask
      
      replyFuture.isDone must beFalse
      theTask.run(timeout)
      replyFuture.isDone must beTrue
      replyFuture.isSuccess must beFalse
      replyFuture.getCause must be_==(TimedoutRequestException)
    }

    "cancel when succeeded" in {
      val replyFuture = timeoutBroker.dispatch(event)      
      timer.theTask must beSomething
      replyFuture.setSuccess()
      there was one(timeout).cancel()
    }
  }
}
