package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

import com.twitter.finagle.util.Conversions._

object ReplyFutureSpec extends Specification with Mockito {
  "ReplyFuture" should {
    val rf = spy(new ReplyFuture)
    val m = "a message to you"
    val more = Reply.More(m, new ReplyFuture)
    val done = Reply.Done(m)

    "setReply sets the reply invokes setSuccess" in {
      rf.setReply(more)
      there was one(rf).setSuccess
      rf.getReply mustEqual more
    }

    "whenDone, with a non-terminal message, passes the function to the next future" in {
      rf.setReply(more)
      var executed = false
      rf.whenDone({ executed = true })
      rf.setReply(Reply.Done(m))
      executed must beFalse
    }

    "whenDone, with a terminal message, executes the function" in {
      rf.setReply(done)
      var executed = false
      rf.whenDone({ executed = true })
      executed mustBe true
    }
  }
}
