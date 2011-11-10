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

  "IdleConnectionHandler with ExactGenerationalQueue" should {

    val queue = new ExactGenerationalQueue[Channel]()
    val handler = new IdleConnectionHandler(100.milliseconds, queue)

    "Don't find a random idle connection amoung fresh connection" in {
      handler.getIdleConnection mustEqual None
      1 to 10 foreach{ _ =>
        val c = mock[Channel]
        handler.addConnection(c)
      }
      handler.getIdleConnection mustEqual None
    }

    "Consider oldest connection as the idlest one" in {
      var t = Time.now

      val connections = (1 to 5).map{ _ => mock[Channel] }
      connections.foreach{ connection =>
        Time.withTimeAt(t) { _ => handler.addConnection(connection) }
        t += 1.millisecond
      }
      Time.withTimeAt(t) { _ =>
        handler.getIdleConnection mustEqual None
      }
      t += handler.getIdleTimeout + 1.millisecond
      Time.withTimeAt(t) { _ =>
        handler.getIdleConnection mustEqual connections.headOption
      }
    }
  }

  "IdleConnectionHandler with BucketGenerationalQueue" should {
    val queue = new BucketGenerationalQueue[Channel](100.milliseconds)
    val handler = new IdleConnectionHandler(100.milliseconds, queue)

    "Don't find a random idle connection amoung fresh connection" in {
      handler.getIdleConnection mustEqual None

      (1 to 10).foreach{ _ =>
        val channel = mock[Channel]
        handler.addConnection(channel)
      }
      handler.getIdleConnection mustEqual None
    }

    "Find a random idle connection" in {
      var t = Time.now
      val channels = Time.withTimeAt(t) { _ =>
        handler.getIdleConnection mustEqual None

        val channels = (1 to 5).map{ _ =>
          val channel = mock[Channel]
          handler.addConnection(channel)
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
        channels.foreach{ handler.removeConnection(_) }
        handler.getIdleConnection mustEq None
      }
    }

    "Find a random idle connection among idle/active connections" in {
      var t = Time.now
      handler.getIdleConnection mustEqual None

      val channels = (1 to 3).map{ _ =>
        val channel = mock[Channel]
        t += handler.getIdleTimeout / 4
        Time.withTimeAt(t) { _ => handler.addConnection(channel) }
        channel
      }

      Time.withTimeAt(t) { _ => handler.getIdleConnection mustEqual None }

      t += handler.getIdleTimeout * 2 + 1.millisecond

      Time.withTimeAt(t) { _ =>
        val someIdleConnection = handler.getIdleConnection
        someIdleConnection mustNotEq None
        channels.contains( someIdleConnection.get ) mustBe true

        // clean-up
        channels.foreach{ handler.removeConnection(_) }
        handler.getIdleConnection mustEq None
      }
    }

    "Generate activity periodically and don't detect this connection as idle" in {
      var t = Time.now
      val channel = Time.withTimeAt(t) { _ =>
        handler.getIdleConnection mustEqual None

        val channel = mock[Channel]
        handler.markConnectionAsActive(channel)
        handler.getIdleConnection mustEqual None
        channel
      }

      (1 to 5).foreach{ _ =>
        t += handler.getIdleTimeout / 2
        Time.withTimeAt(t) { _ =>
          handler.getIdleConnection mustEqual None
          handler.markConnectionAsActive(channel)
        }
      }

      // clean-up
      Time.withTimeAt(t) { _ =>
        handler.removeConnection(channel)
        handler.getIdleConnection mustEq None
      }
    }
  }
}


object ChannelLimitHandlerSpec extends Specification with Mockito {

  def open(handler: ChannelLimitHandler) {
    val connection = mock[Channel]
    val ctx = mock[ChannelHandlerContext]
    ctx.getChannel returns connection

    connection.close answers{ _ =>
      close(handler)
      Channels.future(connection)
    }

    val e = mock[ChannelStateEvent]
    handler.channelOpen(ctx, e)
  }

  def close(handler: ChannelLimitHandler) {
    val connection = mock[Channel]
    val ctx = mock[ChannelHandlerContext]
    ctx.getChannel returns connection

    val e = mock[ChannelStateEvent]
    handler.channelClosed(ctx, e)
  }

  val idleTimeout = 100.milliseconds
  val thresholds = OpenConnectionsThresholds(5, 10, idleTimeout)
  val t0 = Time.now

  // Both implementation of GenerationalQueue must not have effect of the behavior of the
  // ChannelLimitHandler / IdleConnectionHandler, so tests are commons.
  def commonTests(
    queue: GenerationalQueue[Channel],
    idleConnectionHandler: IdleConnectionHandler,
    handler: ChannelLimitHandler
  ) {
    "correctly count connections" in {
      Time.withTimeAt(t0) { _ =>
        handler.openConnections mustEqual 0
        open(handler)
        handler.openConnections mustEqual 1
        close(handler)
        handler.openConnections mustEqual 0
      }
    }

    "refuse connections if total open connections is above highWaterMark" in {
      Time.withTimeAt(t0) { _ =>
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
    }

    "collect idle connection if total open connections is above lowWaterMark" in {
      var t = t0
      handler.openConnections mustEqual 0

      // Generate mocked contexts
      val contexts = (1 to thresholds.highWaterMark) map { i =>
        val channel = mock[Channel]
        val ctx = mock[ChannelHandlerContext]
        ctx.getChannel returns channel
        ctx
      }

      // open all connections
      Time.withTimeAt(t) { _ =>
        contexts foreach { ctx =>
          val e = mock[ChannelStateEvent]
          handler.channelOpen(ctx, e)
          // Simulate response from the service (NB: a connection isn't considered as idle when
          // the server hasn't answer yet the request). So we simulate the server answer by
          // calling directly 'addConnection'
          idleConnectionHandler.addConnection(ctx.getChannel)
        }
        handler.openConnections mustEqual thresholds.highWaterMark
        idleConnectionHandler.getIdleConnections.size mustEqual 0
      }

      // Wait a little
      t += idleTimeout - 1.millisecond
      Time.withTimeAt(t) { _ =>
        // Generate activity on the 5 first connections
        contexts.take(5) foreach { ctx =>
          val e = mock[MessageEvent]
          handler.messageReceived(ctx,e)
        }
      }

      // Wait a little
      t += idleTimeout - 1.milliseconds

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

    "Don't track connection until the server respond to the request" in {
      var t = t0
      handler.openConnections mustEqual 0

      // Generate mocked contexts
      val contexts = (1 to thresholds.highWaterMark) map { i =>
        val channel = mock[Channel]
        val ctx = mock[ChannelHandlerContext]
        ctx.getChannel returns channel
        ctx
      }

      // open all connections
      Time.withTimeAt(t) { _ =>
        contexts foreach { ctx =>
          val e = mock[ChannelStateEvent]
          handler.channelOpen(ctx, e)
        }
        handler.openConnections mustEqual thresholds.highWaterMark
        idleConnectionHandler.getIdleConnections.size mustEqual 0
      }

      // Wait a little
      t += idleTimeout * 3
      Time.withTimeAt(t) { _ =>
        idleConnectionHandler.getIdleConnections.size mustEqual 0
      }

      // Simulate response from server
      Time.withTimeAt(t) { _ =>
        contexts foreach { ctx =>
          idleConnectionHandler.addConnection(ctx.getChannel)
        }
        idleConnectionHandler.getIdleConnections.size mustEqual 0
      }

      // Wait a little
      t += idleTimeout * 3

      // Detect those connections as idle
      Time.withTimeAt(t) { _ =>
        idleConnectionHandler.getIdleConnections.size mustEqual thresholds.highWaterMark
      }
    }
  }

  "ChannelLimitHandler with ExactGenerationalQueue" should {
    Time.withTimeAt(t0) { _ =>
      val queue = new ExactGenerationalQueue[Channel]()
      val idleConnectionHandler = new IdleConnectionHandler(idleTimeout, queue)
      val handler = new ChannelLimitHandler(thresholds, idleConnectionHandler)

      commonTests(queue, idleConnectionHandler, handler)
    }
  }

  "ChannelLimitHandler with BucketGenerationalQueue" should {
    Time.withTimeAt(t0) { _ =>
      val queue = new BucketGenerationalQueue[Channel](idleTimeout)
      val idleConnectionHandler = new IdleConnectionHandler(idleTimeout, queue)
      val handler = new ChannelLimitHandler(thresholds, idleConnectionHandler)

      commonTests(queue, idleConnectionHandler, handler)
    }
  }
}
