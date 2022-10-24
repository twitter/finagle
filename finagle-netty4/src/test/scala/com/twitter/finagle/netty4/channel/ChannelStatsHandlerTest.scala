package com.twitter.finagle.netty4.channel

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Duration
import com.twitter.util.Time
import io.netty.buffer.Unpooled.wrappedBuffer
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import java.util.concurrent.TimeoutException
import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ChannelStatsHandlerTest extends AnyFunSuite with MockitoSugar with Eventually {

  trait SocketTest {
    val chan = mock[Channel]
    val ctx = mock[ChannelHandlerContext]

    when(chan.isWritable).thenReturn(false, true, false)
    when(chan.eventLoop()).thenReturn(new DefaultEventLoop())
    when(ctx.channel).thenReturn(chan)
  }

  private trait InMemoryStatsTest extends SocketTest {
    val sr = new InMemoryStatsReceiver()
    val params = Stack.Params.empty + Stats(sr)
    val handler = new ChannelStatsHandler(new SharedChannelStats(params))
    handler.handlerAdded(ctx)
  }

  test("counters are collected correctly") {
    Time.withCurrentTimeFrozen { control =>
      new InMemoryStatsTest {
        control.advance(5.minutes)
        handler.channelWritabilityChanged(ctx)
        assert(sr.counters(Seq("socket_writable_ms")) == 5.minutes.inMillis)

        control.advance(10.minutes)
        handler.channelWritabilityChanged(ctx)
        assert(sr.counters(Seq("socket_unwritable_ms")) == 10.minutes.inMillis)

        control.advance(20.minutes)
        handler.channelWritabilityChanged(ctx)
        assert(sr.counters(Seq("socket_writable_ms")) == 25.minutes.inMillis)
        assert(sr.counters(Seq("socket_unwritable_ms")) == 10.minutes.inMillis)
      }
    }
  }

  private class TestContext(sharedStats: SharedChannelStats) {
    val ctx = mock[ChannelHandlerContext]
    val channelStatsHandler = new ChannelStatsHandler(sharedStats)
    val chan = new EmbeddedChannel()
    private val start = Time.now
    when(ctx.channel()).thenReturn(chan)

    channelStatsHandler.handlerAdded(ctx)
  }

  private def connectionCountEquals(sr: InMemoryStatsReceiver, num: Float): Unit = {
    assert(sr.gauges(Seq("connections"))() == num)
  }

  test("ChannelStatsHandler counts connections") {
    val sr = new InMemoryStatsReceiver
    val params = Stack.Params.empty + Stats(sr)
    val sharedStats = new SharedChannelStats(params)
    val ctx1 = new TestContext(sharedStats)
    val ctx2 = new TestContext(sharedStats)
    val handler1 = ctx1.channelStatsHandler
    val handler2 = ctx2.channelStatsHandler

    connectionCountEquals(sr, 0)

    handler1.channelActive(ctx1.ctx)
    connectionCountEquals(sr, 1)

    handler2.channelActive(ctx2.ctx)
    connectionCountEquals(sr, 2)

    handler1.channelInactive(ctx1.ctx)
    connectionCountEquals(sr, 1)

    handler2.channelInactive(ctx2.ctx)
    connectionCountEquals(sr, 0)
  }

  test("Multiple close calls only close the channel once") {
    val sr = new InMemoryStatsReceiver
    val params = Stack.Params.empty + Stats(sr)
    val sharedStats = new SharedChannelStats(params)
    val ctx1 = new TestContext(sharedStats)
    val ctx2 = new TestContext(sharedStats)
    val handler1 = ctx1.channelStatsHandler
    val handler2 = ctx2.channelStatsHandler

    def closeCountEquals(num: Long): Unit = {
      val r = sr.counters.get(Seq("closes")).getOrElse(0L)
      assert(r == num)
    }

    closeCountEquals(0)

    handler1.channelActive(ctx1.ctx)
    connectionCountEquals(sr, 1)

    handler2.channelActive(ctx2.ctx)
    connectionCountEquals(sr, 2)

    handler1.close(ctx1.ctx, ctx1.ctx.newPromise)
    closeCountEquals(1)

    // Try to close handler1 again which should have no effect.
    handler1.close(ctx1.ctx, ctx1.ctx.newPromise)
    closeCountEquals(1)

    handler2.close(ctx2.ctx, ctx2.ctx.newPromise)
    closeCountEquals(2)
  }

  test("ChannelStatsHandler handles multiple channelInactive calls") {
    val sr = new InMemoryStatsReceiver
    val params = Stack.Params.empty + Stats(sr)
    val sharedStats = new SharedChannelStats(params)
    val ctx1 = new TestContext(sharedStats)
    val handler = ctx1.channelStatsHandler

    connectionCountEquals(sr, 0)

    handler.channelActive(ctx1.ctx)
    connectionCountEquals(sr, 1)

    handler.channelInactive(ctx1.ctx)
    connectionCountEquals(sr, 0)

    handler.channelInactive(ctx1.ctx)
    connectionCountEquals(sr, 0)
  }

  private def channelLifeCycleTest(
    counterName: String,
    f: (ChannelDuplexHandler, ChannelHandlerContext) => Unit
  ) = test(s"ChannelStatsHandler counts $counterName") {
    val sr = new InMemoryStatsReceiver
    val params = Stack.Params.empty + Stats(sr)
    val sharedStats = new SharedChannelStats(params)
    val ctx = new TestContext(sharedStats)
    val handler = ctx.channelStatsHandler

    assert(sr.counters(Seq(counterName)) == 0)
    f(handler, ctx.ctx)
    assert(sr.counters(Seq(counterName)) == 1)
  }

  channelLifeCycleTest(
    "closes",
    (handler, ctx) => handler.close(ctx, mock[ChannelPromise])
  )

  channelLifeCycleTest(
    "connects",
    (handler, ctx) => handler.channelActive(ctx)
  )

  test("ChannelStatsHandler records connection duration") {
    Time.withCurrentTimeFrozen { control =>
      val sr = new InMemoryStatsReceiver
      val params = Stack.Params.empty + Stats(sr)
      val sharedStats = new SharedChannelStats(params)
      val ctx1 = new TestContext(sharedStats)
      val ctx2 = new TestContext(sharedStats)

      val handler1 = ctx1.channelStatsHandler
      val handler2 = ctx2.channelStatsHandler

      handler1.channelActive(ctx1.ctx)
      handler2.channelActive(ctx2.ctx)
      control.advance(Duration.fromMilliseconds(100))
      handler1.channelInactive(ctx1.ctx)
      assert(sr.stat("connection_duration")() == Seq(100.0))
      control.advance(Duration.fromMilliseconds(200))
      handler2.channelInactive(ctx2.ctx)
      assert(sr.stat("connection_duration")() == Seq(100.0, 300.0))
    }
  }

  test("ChannelStatsHandler counts exceptions") {
    val sr = new InMemoryStatsReceiver
    val params = Stack.Params.empty + Stats(sr)
    val sharedStats = new SharedChannelStats(params)
    val ctx = new TestContext(sharedStats)
    val handler = ctx.channelStatsHandler

    handler.exceptionCaught(ctx.ctx, new RuntimeException)
    handler.exceptionCaught(ctx.ctx, new TimeoutException)
    handler.exceptionCaught(ctx.ctx, new Exception)
    assert(sr.counters(Seq("exn", "java.lang.RuntimeException")) == 1)
    assert(sr.counters(Seq("exn", "java.lang.Exception")) == 1)
    assert(sr.counters(Seq("exn", "java.util.concurrent.TimeoutException")) == 1)
  }

  test("ChannelStatsHandler counts sent and received bytes") {
    val sr = new InMemoryStatsReceiver
    val params = Stack.Params.empty + Stats(sr)
    val sharedStats = new SharedChannelStats(params)
    val ctx1 = new TestContext(sharedStats)
    val handler1 = ctx1.channelStatsHandler

    // note: if `handlerAdded` is called it'd overwrite our setup
    handler1.channelActive(ctx1.ctx)
    handler1.write(ctx1.ctx, wrappedBuffer(Array.fill(42)(0.toByte)), mock[ChannelPromise])
    handler1.channelInactive(ctx1.ctx)

    assert(sr.counter("sent_bytes")() == 42)
    assert(sr.stat("connection_received_bytes")() == Seq(0.0))
    assert(sr.stat("connection_sent_bytes")() == Seq(42.0))

    val ctx2 = new TestContext(sharedStats)
    val handler2 = ctx2.channelStatsHandler

    handler2.channelActive(ctx2.ctx)
    handler2.channelRead(ctx2.ctx, wrappedBuffer(Array.fill(123)(0.toByte)))
    handler2.channelInactive(ctx2.ctx)

    assert(sr.counter("received_bytes")() == 123)
    assert(sr.stat("connection_received_bytes")() == Seq(0.0, 123.0))
    assert(sr.stat("connection_sent_bytes")() == Seq(42.0, 0.0))
  }
}
