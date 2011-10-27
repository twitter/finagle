package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._
import com.twitter.util.Duration
import com.twitter.conversions.time._

object IdleConnectionHandlerSpec extends Specification with Mockito {
  "PreciseIdleConnectionHandler" should {

    val handler = new PreciseIdleConnectionHandler {
      def idleTimeout: Duration = 100.milliseconds
    }

    "Consider oldest connection as the idlest one" in {
      val channels = (1 to 5).map{ _ => mock[Channel] }
      channels.foreach{ channel =>
        handler.markChannelAsActive(channel)
        Thread.sleep(1)
      }
      handler.getIdleConnection mustEqual None
      Thread.sleep(handler.idleTimeout.inMilliseconds + 1)
      handler.getIdleConnection mustEqual channels.headOption
    }
  }

  "BucketIdleConnectionHandler" should {

    val handler = new BucketIdleConnectionHandler {
      def idleTimeout: Duration = 100.milliseconds
    }

    "Find a random idle connection" in {
      handler.getIdleConnection mustEqual None

      val channels = (1 to 5).map{ _ =>
        val channel = mock[Channel]
        handler.markChannelAsActive(channel)
        channel
      }
      handler.getIdleConnection mustEqual None

      // Wait the time channels become idle
      Thread.sleep( 2 * handler.idleTimeout.inMilliseconds + 1)
      val randomIdleConnection = handler.getIdleConnection
      randomIdleConnection mustNotEq None
      channels.contains( randomIdleConnection.get ) mustBe true

      // clean-up
      channels.foreach{ handler.removeChannel(_) }
      handler.getIdleConnection mustEq None
    }

    "Find a random idle connection among idle/active connections" in {
      handler.getIdleConnection mustEqual None

      val channels = (1 to 3).map{ _ =>
        val channel = mock[Channel]
        Thread.sleep( handler.idleTimeout.inMilliseconds / 4 )
        handler.markChannelAsActive(channel)
        channel
      }
      handler.getIdleConnection mustEqual None

      Thread.sleep( 2 * handler.idleTimeout.inMilliseconds + 1 )
      val someIdleConnection = handler.getIdleConnection
      someIdleConnection mustNotEq None
      channels.contains( someIdleConnection.get ) mustBe true

      // clean-up
      channels.foreach{ handler.removeChannel(_) }
      handler.getIdleConnection mustEq None
    }

    "Generate activity every idleTimeout ms and don't detect this connection as idle" in {
      handler.getIdleConnection mustEqual None

      val channel = mock[Channel]
      handler.markChannelAsActive(channel)
      handler.getIdleConnection mustEqual None

      (1 to 5).foreach{ _ =>
        Thread.sleep( handler.idleTimeout.inMilliseconds - 1 )
        handler.getIdleConnection mustEqual None
        handler.markChannelAsActive(channel)
      }

      // clean-up
      handler.removeChannel(channel)
      handler.getIdleConnection mustEq None
    }
  }

}


object ChannelLimitHandlerSpec extends Specification with Mockito {

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
  val idleTimeout = 100.milliseconds

  "ChannelLimitHandlerSpec with PreciseIdleConnectionHandler" should {
    val handler = new ChannelLimitHandler(thresholds, idleTimeout) with PreciseIdleConnectionHandler

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

  "ChannelLimitHandlerSpec with BucketIdleConnectionHandler" should {
    val handler = new ChannelLimitHandler(thresholds, idleTimeout) with BucketIdleConnectionHandler

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
      Thread.sleep(idleTimeout - 1)

      // Generate activity on the 5 first connections
      contexts.take(5).map{ ctx =>
        val e = mock[MessageEvent]
        handler.messageReceived(ctx,e)
      }

      // Wait a little
      Thread.sleep(idleTimeout + 1)

      // try to open a new connection
      val channel = mock[Channel]
      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel
      val e = mock[ChannelStateEvent]
      handler.channelOpen(ctx, e)

      // Check that this new connection hasn't been closed
      there was no(channel).close()

      // Verify that one of the most "idle" connections have been closed
      contexts.drop(5).foreach{ ctx =>
        there was atMostOne(ctx.getChannel).close()
      }
    }

  }
}
