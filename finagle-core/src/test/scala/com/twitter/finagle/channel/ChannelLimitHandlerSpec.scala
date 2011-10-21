package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

object ChannelLimitHandlerSpec extends Specification with Mockito {
  "ChannelLimitHandlerSpec" should {

    def open(handler: ChannelLimitHandler) {
      val channel = mock[Channel]
      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel

      channel.close answers{ _ =>
        close(handler)
        Channels.future(channel)
      }

      val e = mock[ChannelStateEvent]
      handler.channelOpen(ctx, e)
    }

    def close(handler: ChannelLimitHandler) {
      val channel = mock[Channel]
      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel

      val e = mock[ChannelStateEvent]
      handler.channelClosed(ctx, e)
    }

    val thresholds = OpenConnectionsThresholds(5, 10)
    val idleTimeout = 100
    val handler = new ChannelLimitHandler(thresholds, idleTimeout, 1)

    "correctly count connections" in {
      handler.openConnections mustEqual 0
      open(handler)
      handler.openConnections mustEqual 1
      close(handler)
      handler.openConnections mustEqual 0
    }

    "refuse connections if total open connections is above highWaterMark" in {
      // Load up
      handler.openConnections mustEqual 0
      (1 to thresholds.highWaterMark).map{ i =>
        open(handler)
      }
      handler.openConnections mustEqual thresholds.highWaterMark

      // Try to open a new connection
      open(handler)
      handler.openConnections mustEqual thresholds.highWaterMark

      // clean up
      (1 to thresholds.highWaterMark).map{ i =>
        close(handler)
      }
      handler.openConnections mustEqual 0
    }

    "collect idle connection if total open connections is above lowWaterMark" in {
      handler.openConnections mustEqual 0

      // Generate mocked contexts
      val contexts = (1 to thresholds.highWaterMark).map{ i =>
        val channel = mock[Channel]
        val ctx = mock[ChannelHandlerContext]
        ctx.getChannel returns channel
        ctx
      }

      // open all connections (every ms, so that we know which one will be detected as idle)
      contexts.map{ ctx =>
        val e = mock[ChannelStateEvent]
        handler.channelOpen(ctx, e)
        Thread.sleep(1)
      }
      handler.openConnections mustEqual thresholds.highWaterMark

      // Wait a little
      Thread.sleep(idleTimeout / 2)

      // Generate activity on the 5 first connections
      contexts.take(5).map{ ctx =>
        val e = mock[MessageEvent]
        handler.messageReceived(ctx,e)
      }

      // Wait a little
      Thread.sleep(idleTimeout / 2)

      // try to open a new connection
      val channel = mock[Channel]
      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel
      val e = mock[ChannelStateEvent]
      handler.channelOpen(ctx, e)

      // Check that this new connection hasn't been closed
      there was no(channel).close()

      // Verify that the most "idle" connection have been closed
      there was one(contexts(5).getChannel).close()

      // but not the next one
      there was no(contexts(6).getChannel).close()
    }
  }
}
