package com.twitter.finagle.integration

import com.twitter.finagle.{CancelledConnectionException, WriteException}
import com.twitter.util.Await
import org.jboss.netty.channel._
import org.mockito.ArgumentCaptor
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito


class CancellationSpec extends SpecificationWithJUnit with IntegrationBase with Mockito {
  "Cancellation" should {
    "cancel while waiting for a reply" in {
      val m = new MockChannel
      val cli = m.build()

      val f = cli("123")
      f.isDefined must beFalse
      m.connectFuture.setSuccess()
      m.channel.isOpen returns true

      f.isDefined must beFalse

      // the request was sent.
      val meCaptor = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
      there was one(m.channelPipeline).sendDownstream(meCaptor.capture)
      meCaptor.getValue must beLike {
        case event: DownstreamMessageEvent =>
          event.getChannel must be_==(m.channel)
          event.getMessage must beLike {
            case s: String => s == "123"
          }
      }

      f.raise(new Exception)
      val seCaptor = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
      there were two(m.channelPipeline).sendDownstream(seCaptor.capture)
      seCaptor.getValue must beLike {
        case event: DownstreamChannelStateEvent =>
          event.getChannel must be_==(m.channel)
          event.getState must be_==(ChannelState.OPEN)
          event.getValue must be_==(java.lang.Boolean.FALSE)
      }
    }

    "cancel while waiting in the queue" in {
      val m = new MockChannel
      val client = m.build()
      m.connectFuture.setSuccess()

      there was no(m.channelPipeline).sendDownstream(any)
      val f0 = client("123")
      f0.isDefined must beFalse
      there was one(m.channelPipeline).sendDownstream(any)
      val f1 = client("333")
      f1.isDefined must beFalse
      there was one(m.channelPipeline).sendDownstream(any)

      f1.raise(new Exception)
      f1.isDefined must beTrue
      Await.result(f1) must throwA[CancelledConnectionException]
    }
  }
}
