package com.twitter.finagle.integration

import java.util.concurrent.Executors

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import org.jboss.netty.channel._

import com.twitter.finagle.builder.{ClientBuilder, ReferenceCountedChannelFactory}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.Ok
import com.twitter.finagle.{WriteException, CancelledConnectionException}

import com.twitter.conversions.time._

object CancellationSpec extends Specification with IntegrationBase with Mockito {
  "Cancellation" should {
    "cancel while waiting for connect()" in {
      val m = new MockChannel
      val client = m.build()
      val f = client("123")
      f.isDefined must beFalse
      there was no(m.connectFuture).cancel()
      m.connectFuture.isCancelled must beFalse
      f.cancel()
      there was one(m.connectFuture).cancel()
      m.connectFuture.isCancelled must beTrue
      f.isDefined must beTrue
      f() must throwA(new WriteException(new CancelledConnectionException))
    }

    "cancel while waiting for a reply" in {
      val m = new MockChannel
      val client = m.build()
      val f = client("123")
      f.isDefined must beFalse
      m.connectFuture.setSuccess()
      m.channel.isOpen returns true

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

      f.cancel()
      val seCaptor = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
      there were two(m.channelPipeline).sendDownstream(seCaptor.capture)
      seCaptor.getValue must beLike {
        case event: DownstreamChannelStateEvent =>
          event.getChannel must be_==(m.channel)
          event.getState must be_==(ChannelState.OPEN)
          event.getValue must be_==(java.lang.Boolean.FALSE)
      }
    }

    "cancel white waiting in the queue" in {
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

      f1.cancel()
      f1.isDefined must beTrue
      f1() must throwA[CancelledConnectionException]
    }
  }
}
