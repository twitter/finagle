package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._
import com.twitter.conversions.time._
import com.twitter.util.{Time, Duration}

object GenerationalQueueSpec extends Specification with Mockito {

  def genericGenerationalQueueTest[A](queue: GenerationalQueue[String], timeout: Duration) {

    "Don't collect fresh data" in {
      Time.withCurrentTimeFrozen { _ =>
        queue.add("foo")
        queue.add("bar")
        val collectedValue = queue.collect(timeout)
        collectedValue mustBe None
      }
    }

    "Don't collect old data recently refreshed" in {
      var t = Time.now
      Time.withTimeAt(t) { _ => queue.add("foo") }
      t += timeout * 3
      Time.withTimeAt(t) { _ => queue.add("bar") }
      t += timeout * 3
      Time.withTimeAt(t) { _ => queue.touch("foo") }
      t += timeout * 3
      Time.withTimeAt(t) { _ =>
        val collectedValue = queue.collect(timeout)
        collectedValue mustNotBe None
        collectedValue mustEqual Some("bar")
      }
    }

    "collectAll old data" in {
      var t = Time.now
      Time.withTimeAt(t) { _ =>
        queue.add("foo")
        queue.add("bar")
      }
      t += timeout
      Time.withTimeAt(t) { _ =>
        queue.add("foo2")
        queue.add("bar2")
      }
      t += timeout
      Time.withTimeAt(t) { _ =>
        val collectedValues = queue.collectAll(timeout + 1.millisecond)
        collectedValues.size mustEqual 2
        collectedValues mustContain "foo"
        collectedValues mustContain "bar"
      }
    }
  }

  "ExactGenerationalQueue" should {
    val queue = new ExactGenerationalQueue[String]()
    genericGenerationalQueueTest(queue, 100.milliseconds)
  }

  "BucketGenerationalQueue" should {
    val timeout = 100.milliseconds
    val queue = new BucketGenerationalQueue[String](timeout)
    genericGenerationalQueueTest(queue, timeout)
  }

}

object IdleConnectionHandlerSpec extends Specification with Mockito {

  def time[A]( f : => A ) = {
    val start = System.currentTimeMillis()
    val result = f
    val end = System.currentTimeMillis()
    (result, end - start)
  }

  "PreciseIdleConnectionHandler" should {

    val handler = new PreciseIdleConnectionHandler(100.milliseconds)

    "Don't find a random idle connection amoung fresh connection" in {
      handler.getIdleConnection mustEqual None
      1 to 10 foreach{ _ =>
        val channel = mock[Channel]
        handler.markChannelAsActive(channel)
      }
      handler.getIdleConnection mustEqual None
    }

    "Consider oldest connection as the idlest one" in {
      var t = Time.now

      val channels = (1 to 5).map{ _ => mock[Channel] }
      channels.foreach{ channel =>
        Time.withTimeAt(t) { _ => handler.markChannelAsActive(channel) }
        t += 1.millisecond
      }
      Time.withTimeAt(t) { _ =>
        handler.getIdleConnection mustEqual None
      }
      t += handler.getIdleTimeout + 1.millisecond
      Time.withTimeAt(t) { _ =>
        handler.getIdleConnection mustEqual channels.headOption
      }
    }
  }

  "BucketIdleConnectionHandler" should {

    val handler = new BucketIdleConnectionHandler(100.milliseconds)

    "Don't find a random idle connection amoung fresh connection" in {
      handler.getIdleConnection mustEqual None

      (1 to 10).foreach{ _ =>
        val channel = mock[Channel]
        handler.markChannelAsActive(channel)
      }
      handler.getIdleConnection mustEqual None
    }

    "Find a random idle connection" in {
      var t = Time.now
      val channels = Time.withTimeAt(t) { _ =>
        handler.getIdleConnection mustEqual None

        val channels = (1 to 5).map{ _ =>
          val channel = mock[Channel]
          handler.markChannelAsActive(channel)
          channel
        }
        handler.getIdleConnection mustEqual None
        channels
      }

      // Wait the time channels become idle
      t += (2 * handler.getIdleTimeout).milliseconds + 1.millisecond

      Time.withTimeAt(t) { _ =>
        val randomIdleConnection = handler.getIdleConnection
        randomIdleConnection mustNotEq None
        channels.contains( randomIdleConnection.get ) mustBe true

        // clean-up
        channels.foreach{ handler.removeChannel(_) }
        handler.getIdleConnection mustEq None
      }
    }

    "Find a random idle connection among idle/active connections" in {
      var t = Time.now
      handler.getIdleConnection mustEqual None

      val channels = (1 to 3).map{ _ =>
        val channel = mock[Channel]
        t += handler.getIdleTimeout / 4
        Time.withTimeAt(t) { _ => handler.markChannelAsActive(channel) }
        channel
      }

      Time.withTimeAt(t) { _ => handler.getIdleConnection mustEqual None }

      t += handler.getIdleTimeout * 2 + 1.millisecond

      Time.withTimeAt(t) { _ =>
        val someIdleConnection = handler.getIdleConnection
        someIdleConnection mustNotEq None
        channels.contains( someIdleConnection.get ) mustBe true

        // clean-up
        channels.foreach{ handler.removeChannel(_) }
        handler.getIdleConnection mustEq None
      }
    }

    "Generate activity every idleTimeout ms and don't detect this connection as idle" in {
      var t = Time.now
      val channel = Time.withTimeAt(t) { _ =>
        handler.getIdleConnection mustEqual None

        val channel = mock[Channel]
        handler.markChannelAsActive(channel)
        handler.getIdleConnection mustEqual None
        channel
      }

      (1 to 5).foreach{ _ =>
        t += handler.getIdleTimeout / 2
        Time.withTimeAt(t) { _ =>
          handler.getIdleConnection mustEqual None
          handler.markChannelAsActive(channel)
        }
      }

      // clean-up
      Time.withTimeAt(t) { _ =>
        handler.removeChannel(channel)
        handler.getIdleConnection mustEq None
      }
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
    val idleConnectionHandler = new PreciseIdleConnectionHandler(idleTimeout)
    val handler = new ChannelLimitHandler(thresholds, idleConnectionHandler)

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
      var t = Time.now
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
        Time.withTimeAt(t) { _ => handler.channelOpen(ctx, e) }
        t += 1.millisecond
      }
      Time.withTimeAt(t) { _ =>
        handler.openConnections mustEqual thresholds.highWaterMark
      }

      // Wait a little
      t += idleTimeout / 2
      Time.withTimeAt(t) { _ =>
        // Generate activity on the 5 first connections
        contexts.take(5).map{ ctx =>
          val e = mock[MessageEvent]
          handler.messageReceived(ctx,e)
        }
      }

      // Wait a little
      t += idleTimeout / 2

      // try to open a new connection
      Time.withTimeAt(t) { _ =>
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

  "ChannelLimitHandlerSpec with BucketIdleConnectionHandler" should {
    val idleConnectionHandler = new BucketIdleConnectionHandler(idleTimeout)
    val handler = new ChannelLimitHandler(thresholds, idleConnectionHandler)

    "collect idle connection if total open connections is above lowWaterMark" in {
      var t = Time.now

      val contexts = Time.withTimeAt(t) { _ =>
        handler.openConnections mustEqual 0

        // Generate mocked contexts
        (1 to thresholds.highWaterMark).map{ i =>
          val channel = mock[Channel]
          val ctx = mock[ChannelHandlerContext]
          ctx.getChannel returns channel
          ctx
        }
      }

      Time.withTimeAt(t) { _ =>
        contexts.map{ ctx =>
          val e = mock[ChannelStateEvent]
          handler.channelOpen(ctx, e)
        }
      }

      Time.withTimeAt(t) { _ => handler.openConnections mustEqual thresholds.highWaterMark }

      // Wait a little
      t += idleTimeout / 2

      // Generate activity on the 5 first connections
      Time.withTimeAt(t) { _ =>
        contexts.take(5).map{ ctx =>
          val e = mock[MessageEvent]
          handler.messageReceived(ctx,e)
        }
      }

      // Wait a little
      t += idleTimeout + 1.millisecond

      // try to open a new connection
      Time.withTimeAt(t) { _ =>
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
}
