package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._
import java.net.SocketAddress

class RetryingBrokerSpec extends Specification with Mockito {
  class MyException extends Exception

  "RetryingBroker" should {
    val brokeredChannel = mock[BrokeredChannel]
    val tries = 3
    val message = 1
    val writeFuture = mock[ChannelFuture]
    val e = new DownstreamMessageEvent(mock[Channel], writeFuture, message, mock[SocketAddress])

    "retries up to $tries times" in {
      var invocations = 0
      val exception = new MyException
      val underlying = new Broker {
        def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent) {
          invocations += 1
          handlingChannel mustBe brokeredChannel
          e.getFuture.setFailure(exception)
        }
      }
      val retryingBroker = new RetryingBroker(underlying, tries)

      retryingBroker.dispatch(brokeredChannel, e)
      there was one(writeFuture).setFailure(exception)
      invocations mustEqual 3
    }
  }
}