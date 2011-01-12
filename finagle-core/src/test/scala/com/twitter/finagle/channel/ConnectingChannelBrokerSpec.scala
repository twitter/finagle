package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import org.jboss.netty.channel._

import com.twitter.util.{Promise, Return}

object ConnectingChannelBrokerSpec extends Specification with Mockito {
  "ConnectingChannelBroker" should {
    val pipeline = mock[ChannelPipeline]
    val channel = mock[Channel]
    val adapter = mock[BrokerAdapter]
    val message = mock[Object]
    val replyFutureCaptor = ArgumentCaptor.forClass(classOf[Promise[Any]])
    channel.getPipeline returns pipeline
    pipeline.getLast returns adapter

    lazy val replyFuture = replyFutureCaptor.getValue

    val channelFuture = Channels.future(channel)

    // Mockito can't spy on anonymous classes, so we have to make a
    // proper one.
    class FakeConnectingChannelBroker extends ConnectingChannelBroker[Any, Any] {
      def getChannel = channelFuture
      def putChannel(ch: Channel) {}
    }
    val broker = spy(new FakeConnectingChannelBroker)

    "fail with a WriteException when channel acquisition fails" in {
      val f = broker(message)
      f.isDefined must beFalse
      channelFuture.setFailure(new Exception("wtf"))
      f() must throwA(new WriteException(new Exception("wtf")))
      there was no(adapter).writeAndRegisterReply(
        Matchers.eq(channel), Matchers.eq(message), any[Promise[Any]])
      there was no(broker).putChannel(any[Channel])
    }

    "write the request to the broker adapter when acquisition is successful" in {
      val f = broker(message)
      channelFuture.setSuccess()
      f.isDefined must beFalse

      there was one(adapter).writeAndRegisterReply(
        Matchers.eq(channel), Matchers.eq(message), replyFutureCaptor.capture)
      there was no(broker).putChannel(any[Channel])
    }

    "put the channel back upon completion" in {
      val f = broker(message)
      there was no(broker).putChannel(any[Channel])
      channelFuture.setSuccess()
      there was one(adapter).writeAndRegisterReply(
        Matchers.eq(channel), Matchers.eq(message), replyFutureCaptor.capture)
      replyFuture() = Return("hey")
      there was one(broker).putChannel(channel)
    }

    "freak out if the channel doesn't have a BrokerAdapter" in {
      pipeline.getLast returns null
      channelFuture.setSuccess()
      broker(message)() must throwA(new InvalidPipelineException)
    }
  }
}
