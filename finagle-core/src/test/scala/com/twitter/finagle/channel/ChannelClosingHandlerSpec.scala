package com.twitter.finagle.channel

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import org.jboss.netty.channel.{
  Channels, Channel, ChannelHandlerContext,
  ChannelStateEvent, ChannelPipeline}

class ChannelClosingHandlerSpec extends SpecificationWithJUnit with Mockito {
  "ChannelClosingHandler" should {
    val channel = mock[Channel]
    val closeFuture = Channels.future(channel)
    channel.close() returns closeFuture
    val handler = new ChannelClosingHandler
    val ctx = mock[ChannelHandlerContext]
    val e = mock[ChannelStateEvent]
    val pipeline = mock[ChannelPipeline]

    pipeline.isAttached returns true

    ctx.getPipeline returns pipeline
    ctx.getChannel returns channel

    "close the channel immediately" in {
      "channel is already open" in {
        handler.channelOpen(ctx, e)
        there was no(channel).close()
        handler.close()
        there was one(channel).close()
      }

      "channel is attached" in {
        handler.beforeAdd(ctx)
        there was no(channel).close()
        handler.close()
        there was one(channel).close()
      }
    }

    "delay closing until it has been opened" in {
      handler.close()
      there was no(channel).close()

      "before channel has been opened" in {
        handler.channelOpen(ctx, e)
        there was one(channel).close()
      }

      "before channel has been attached" in {
        handler.beforeAdd(ctx)
        there was one(channel).close()        
      }
    }
  }
}
