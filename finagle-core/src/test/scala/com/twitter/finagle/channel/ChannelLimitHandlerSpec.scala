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
      Time.withTimeFunction(t) { _ =>
        queue.add("foo")
        t += timeout * 3
        queue.add("bar")
        t += timeout * 3
        queue.touch("foo")
        t += timeout * 3
        val collectedValue = queue.collect(timeout)
        collectedValue mustNotBe None
        collectedValue mustEqual Some("bar")
      }
    }

    "collectAll old data" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        queue.add("foo")
        queue.add("bar")

        t += timeout
        queue.add("foo2")
        queue.add("bar2")

        t += timeout
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
      handler.get mustEqual None
      1 to 10 foreach{ _ =>
        val c = mock[Channel]
        handler.put(c)
      }
      handler.get mustEqual None
    }

    "Consider oldest connection as the idlest one" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        val connections = (1 to 5).map{ _ => mock[Channel] }
        connections.foreach{ connection =>
          handler.put(connection)
          t += 1.millisecond
        }

        handler.get mustEqual None

        t += handler.timeout + 1.millisecond
        handler.get mustEqual connections.headOption
      }
    }
  }

  "IdleConnectionHandler with BucketGenerationalQueue" should {
    val queue = new BucketGenerationalQueue[Channel](100.milliseconds)
    val handler = new IdleConnectionHandler(100.milliseconds, queue)

    "Don't find a random idle connection amoung fresh connection" in {
      handler.get mustEqual None

      (1 to 10).foreach{ _ =>
        val channel = mock[Channel]
        handler.put(channel)
      }
      handler.get mustEqual None
    }

    "Find a random idle connection" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        handler.get mustEqual None

        val channels = (1 to 5).map{ _ =>
          val channel = mock[Channel]
          handler.put(channel)
          channel
        }
        handler.get mustEqual None
        channels

      // Wait the time channels become idle
        t += (2 * handler.timeout).milliseconds + 1.millisecond

        val randomIdleConnection = handler.get
        randomIdleConnection mustNotEq None
        channels.contains( randomIdleConnection.get ) mustBe true

        // clean-up
        channels.foreach{ handler.remove(_) }
        handler.get mustEq None
      }
    }

    "Find a random idle connection among idle/active connections" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        handler.get mustEqual None

        val channels = (1 to 3).map{ _ =>
          val channel = mock[Channel]
          t += handler.timeout / 4
          handler.put(channel)
          channel
        }

        handler.get mustEqual None

        t += handler.timeout * 2 + 1.millisecond

        val someIdleConnection = handler.get
        someIdleConnection mustNotEq None
        channels.contains( someIdleConnection.get ) mustBe true

        // clean-up
        channels.foreach{ handler.remove(_) }
        handler.get mustEq None
      }
    }

    "Generate activity periodically and don't detect this connection as idle" in {
      var t = Time.now
      Time.withTimeFunction(t) { _ =>
        handler.get mustEqual None

        val channel = mock[Channel]
        handler.activate(channel)
        handler.get mustEqual None
        channel

        (1 to 5).foreach{ _ =>
          t += handler.timeout / 2
          handler.get mustEqual None
          handler.activate(channel)
        }

        // clean-up
        handler.remove(channel)
        handler.get mustEq None
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
      Time.withTimeFunction(t) { _ =>
        handler.openConnections mustEqual 0

        // Generate mocked contexts
        val contexts = (1 to thresholds.highWaterMark) map { i =>
          val channel = mock[Channel]
          val ctx = mock[ChannelHandlerContext]
          ctx.getChannel returns channel
          ctx
        }

        // open all connections
        contexts foreach { ctx =>
          val e = mock[ChannelStateEvent]
          handler.channelOpen(ctx, e)
          // Simulate response from the service (NB: a connection isn't considered as idle when
          // the server hasn't answer yet the request). So we simulate the server answer by
          // calling directly 'put'
          idleConnectionHandler.put(ctx.getChannel)
        }
        handler.openConnections mustEqual thresholds.highWaterMark
        idleConnectionHandler.size mustEqual 0

        // Wait a little
        t += idleTimeout - 1.millisecond
        // Generate activity on the 5 first connections
        contexts.take(5) foreach { ctx =>
          val e = mock[MessageEvent]
          handler.messageReceived(ctx,e)
        }

        // Wait a little
        t += idleTimeout - 1.milliseconds

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

    "Don't track connection until the server respond to the request" in {
      var t = t0
      Time.withTimeFunction(t) { _ =>
        handler.openConnections mustEqual 0

        // Generate mocked contexts
        val contexts = (1 to thresholds.highWaterMark) map { i =>
          val channel = mock[Channel]
          val ctx = mock[ChannelHandlerContext]
          ctx.getChannel returns channel
          ctx
        }

        // open all connections
        contexts foreach { ctx =>
          val e = mock[ChannelStateEvent]
          handler.channelOpen(ctx, e)
        }
        handler.openConnections mustEqual thresholds.highWaterMark
        idleConnectionHandler.size mustEqual 0

        // Wait a little
        t += idleTimeout * 3
        idleConnectionHandler.size mustEqual 0

        // Simulate response from server
        contexts foreach { ctx =>
          idleConnectionHandler.put(ctx.getChannel)
        }
        idleConnectionHandler.size mustEqual 0

        // Wait a little
        t += idleTimeout * 3

        // Detect those connections as idle
        idleConnectionHandler.size mustEqual thresholds.highWaterMark
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
