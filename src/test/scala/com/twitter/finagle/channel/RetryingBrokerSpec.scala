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
    val brokeredChannel = new BrokeredChannelFactory().newChannel(Channels.pipeline())

    "when it never succeeds" in {
      val underlying = new Broker {
        def dispatch(brokeredChannel: BrokeredChannel, e: MessageEvent) {
          invocations += 1
          e.getFuture.setFailure(exception)
        }
      }
      brokeredChannel.connect(new RetryingBroker(underlying, tries))

      "retries up to $tries times" in {
        val writeFuture = Channels.write(brokeredChannel, someMessage)
        writeFuture.await()
        writeFuture.getCause must haveClass[MyException]
        invocations mustEqual 3
      }
    }

    "when it eventually succeeds" in {
      val underlying = new Broker {
        def dispatch(brokeredChannel: BrokeredChannel, e: MessageEvent) {
          invocations += 1
          val future = e.getFuture
          if (invocations < 3) future.setFailure(exception)
          else future.setSuccess()
        }
      }
      brokeredChannel.connect(new RetryingBroker(underlying, tries))

      "retries until it succeeds" in {
        val writeFuture = Channels.write(brokeredChannel, someMessage)
        writeFuture.await()
        writeFuture.isSuccess mustBe true
        invocations mustEqual 3
      }
    }
  }
}