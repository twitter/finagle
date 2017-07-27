package com.twitter.finagle.netty4.channel

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.TimeConversions.intToTimeableNumber
import com.twitter.util.{Duration, Stopwatch, Time}
import io.netty.buffer.Unpooled.wrappedBuffer
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.util.{Attribute, AttributeKey}
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.LongAdder
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class ChannelStatsHandlerTest extends FunSuite with MockitoSugar {
  def mkAttr[T](initial: T): Attribute[T] = new Attribute[T] {
    var _v = initial
    def set(value: T): Unit = _v = value
    def key(): AttributeKey[T] = ???
    def get(): T = _v
    def getAndRemove(): T = ???
    def remove(): Unit = ???
    def compareAndSet(oldValue: T, newValue: T): Boolean = ???
    def setIfAbsent(value: T): T = ???
    def getAndSet(value: T): T = ???
  }

  trait SocketTest {
    val chan = mock[Channel]
    val ctx = mock[ChannelHandlerContext]

    when(chan.isWritable).thenReturn(false, true, false)
    when(ctx.channel).thenReturn(chan)
    when(chan.attr(ChannelStatsHandler.ChannelWasWritableKey)).thenReturn(mkAttr(true))
    when(chan.attr(ChannelStatsHandler.ChannelWritableDurationKey))
      .thenReturn(mkAttr(Stopwatch.start()))
  }

  trait InMemoryStatsTest extends SocketTest {
    val sr = new InMemoryStatsReceiver()
    val handler = new ChannelStatsHandler(sr)
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

  private class TestContext(
    bytesReceived: LongAdder = new LongAdder(),
    bytesWritten: LongAdder = new LongAdder()
  ) {
    val ctx = mock[ChannelHandlerContext]
    private val chan = new EmbeddedChannel()
    private val start = Time.now

    when(ctx.channel).thenReturn(chan)
    chan
      .attr(ChannelStatsHandler.ConnectionStatsKey)
      .set(ChannelStats(bytesReceived, bytesWritten))

    val durationAttr: () => Duration = () => Time.now - start
    chan.attr(ChannelStatsHandler.ConnectionDurationKey).set(durationAttr)
    chan.attr(ChannelStatsHandler.ChannelWasWritableKey).set(true)
    chan.attr(ChannelStatsHandler.ChannelWritableDurationKey).set(Stopwatch.start())
  }

  private def connectionCountEquals(sr: InMemoryStatsReceiver, num: Float): Unit = {
    assert(sr.gauges(Seq("connections"))() == num)
  }

  test("ChannelStatsHandler counts connections") {
    val sr = new InMemoryStatsReceiver
    val handler = new ChannelStatsHandler(sr)
    val ctx1 = new TestContext().ctx
    val ctx2 = new TestContext().ctx

    connectionCountEquals(sr, 0)

    handler.channelActive(ctx1)
    connectionCountEquals(sr, 1)

    handler.channelActive(ctx2)
    connectionCountEquals(sr, 2)

    handler.channelInactive(ctx1)
    connectionCountEquals(sr, 1)

    handler.channelInactive(ctx2)
    connectionCountEquals(sr, 0)
  }

  test("ChannelStatsHandler handles multiple channelInactive calls") {
    val sr = new InMemoryStatsReceiver
    val handler = new ChannelStatsHandler(sr)
    val ctx1 = new TestContext().ctx

    connectionCountEquals(sr, 0)

    handler.channelActive(ctx1)
    connectionCountEquals(sr, 1)

    handler.channelInactive(ctx1)
    connectionCountEquals(sr, 0)

    handler.channelInactive(ctx1)
    connectionCountEquals(sr, 0)
  }

  private def channelLifeCycleTest(
    counterName: String,
    f: (ChannelDuplexHandler, ChannelHandlerContext) => Unit
  ) = test(s"ChannelStatsHandler counts $counterName") {
    val sr = new InMemoryStatsReceiver
    val handler = new ChannelStatsHandler(sr)
    val ctx = new TestContext().ctx

    assert(!sr.counters.contains(Seq(counterName)))
    f(handler, ctx)
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
      val handler = new ChannelStatsHandler(sr)
      val ctx1 = new TestContext().ctx
      val ctx2 = new TestContext().ctx

      handler.channelActive(ctx1)
      handler.channelActive(ctx2)
      control.advance(Duration.fromMilliseconds(100))
      handler.channelInactive(ctx1)
      assert(sr.stat("connection_duration")() == Seq(100.0))
      control.advance(Duration.fromMilliseconds(200))
      handler.channelInactive(ctx2)
      assert(sr.stat("connection_duration")() == Seq(100.0, 300.0))
    }
  }

  test("ChannelStatsHandler counts exceptions") {
    val sr = new InMemoryStatsReceiver
    val handler = new ChannelStatsHandler(sr)
    val ctx = new TestContext().ctx

    handler.exceptionCaught(ctx, new RuntimeException)
    handler.exceptionCaught(ctx, new TimeoutException)
    handler.exceptionCaught(ctx, new Exception)
    assert(sr.counters(Seq("exn", "java.lang.RuntimeException")) == 1)
    assert(sr.counters(Seq("exn", "java.lang.Exception")) == 1)
    assert(sr.counters(Seq("exn", "java.util.concurrent.TimeoutException")) == 1)
  }

  test("ChannelStatsHandler counts sent and received bytes") {
    val sr = new InMemoryStatsReceiver
    val handler = new ChannelStatsHandler(sr)
    val bytesRead = new LongAdder()
    val bytesWritten = new LongAdder()
    val ctx1 = new TestContext(bytesRead, bytesWritten).ctx

    val ch = ctx1.channel
    val attr = ch.attr(ChannelStatsHandler.ConnectionStatsKey)

    // note: if `handlerAdded` is called it'd overwrite our setup
    handler.channelActive(ctx1)
    handler.write(ctx1, wrappedBuffer(Array.fill(42)(0.toByte)), mock[ChannelPromise])
    handler.channelInactive(ctx1)

    assert(sr.counter("sent_bytes")() == 42)
    assert(bytesWritten.sum() == 42)
    assert(sr.stat("connection_received_bytes")() == Seq(0.0))
    assert(sr.stat("connection_sent_bytes")() == Seq(42.0))

    bytesRead.reset()
    bytesWritten.reset()

    val ctx2 = new TestContext(bytesRead, bytesWritten).ctx
    handler.channelActive(ctx2)
    handler.channelRead(ctx2, wrappedBuffer(Array.fill(123)(0.toByte)))
    handler.channelInactive(ctx2)

    assert(sr.counter("received_bytes")() == 123)
    assert(bytesRead.sum() == 123)
    assert(sr.stat("connection_received_bytes")() == Seq(0.0, 123.0))
    assert(sr.stat("connection_sent_bytes")() == Seq(42.0, 0.0))
  }
}
