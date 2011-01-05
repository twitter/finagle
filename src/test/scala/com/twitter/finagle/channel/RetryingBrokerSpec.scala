package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

class RetryingBrokerSpec extends Specification with Mockito {
  class MyException extends Exception

  "RetryingBroker" should {
    val exception = new MyException
    var invocations = 0
    val tries = 3
    val someMessage = mock[Object]
    val pipeline = Channels.pipeline()
    pipeline.addLast("silenceWarnings", new SimpleChannelUpstreamHandler {
      override def exceptionCaught(ctx: ChannelHandlerContext, exc: ExceptionEvent) {}
    })
    val brokeredChannel = new BrokeredChannelFactory().newChannel(pipeline)

    "when it never succeeds" in {
      val underlying = new Broker {
        def dispatch(e: MessageEvent) = {
          invocations += 1
          e.getFuture.setFailure(exception)
          ReplyFuture.failed(exception)
        }
      }
      brokeredChannel.connect(new RetryingBroker(underlying, tries))

      "retries up to $tries times" in {
        val writeFuture = Channels.write(brokeredChannel, someMessage)
        writeFuture.await()
        writeFuture.getCause must haveClass[MyException]
        invocations mustEqual 3
      }

      "apply retries on each new request" in {
        for (_ <- 0 until 3) {
          invocations = 0
          val f = Channels.write(brokeredChannel, someMessage)
          f.await()
          f.getCause must haveClass[MyException]
          invocations mustEqual 3
        }
      }
    }

    "when it eventually succeeds" in {
      val underlying = new Broker {
        def dispatch(e: MessageEvent) = {
          invocations += 1
          val future = e.getFuture
          if (invocations < 3) {
            future.setFailure(exception)
            ReplyFuture.failed(exception)
          } else {
            future.setSuccess()
            ReplyFuture.success(someMessage)
          }
        }
      }
      brokeredChannel.connect(new RetryingBroker(underlying, tries))

      "retries until it succeeds" in {
        val writeFuture = Channels.write(brokeredChannel, someMessage)
        writeFuture.await()
        writeFuture.isSuccess mustBe true
        invocations mustEqual 3
      }

      "apply retries on each new request" in {
        for (_ <- 0 until 3) {
          invocations = 0
          val f = Channels.write(brokeredChannel, someMessage)
          f.await()
          f.isSuccess must beTrue
          invocations mustEqual 3
        }
      }
    }
  }
}
