package com.twitter.finagle.netty4.channel

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.TimeConversions.intToTimeableNumber
import com.twitter.util.{Time, Duration, Stopwatch}
import io.netty.buffer.Unpooled.wrappedBuffer
import io.netty.channel._
import io.netty.util.{AttributeKey, Attribute}
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ChannelStatsHandlerTest extends FunSuite with MockitoSugar {
  def mkAttr[T](initial: T): Attribute[T] = new Attribute[T]{
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
    when(ctx.attr(ChannelStatsHandler.ChannelWasWritableKey)).thenReturn(mkAttr(true))
    when(ctx.attr(ChannelStatsHandler.ChannelWritableDurationKey)).thenReturn(mkAttr(Stopwatch.start()))
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

  trait TestContext {
    val sr = new InMemoryStatsReceiver
    val handler = new ChannelStatsHandler(sr)
    val ctx = mock[ChannelHandlerContext]
    val bytesWritten = new AtomicLong(0)
    val bytesReceived = new AtomicLong(0)

    def resetConnection(): Unit = {
      bytesReceived.set(0)
      bytesWritten.set(0)
    }

    val start = Time.now
    val statsAttr = mock[Attribute[ChannelStats]]
    when(ctx.attr(ChannelStatsHandler.ConnectionStatsKey)).thenReturn(statsAttr)
    when(statsAttr.get).thenReturn(ChannelStats(bytesReceived, bytesWritten))

    val durationAttr = mock[Attribute[() => Duration]]
    when(durationAttr.get).thenReturn(() => Time.now - start)
    when(ctx.attr(ChannelStatsHandler.ConnectionDurationKey)).thenReturn(durationAttr)
    when(ctx.attr(ChannelStatsHandler.ChannelWasWritableKey)).thenReturn(mkAttr(true))
    when(ctx.attr(ChannelStatsHandler.ChannelWritableDurationKey)).thenReturn(mkAttr(Stopwatch.start()))
  }


  test("ChannelStatsHandler counts connections") {
    val c = new TestContext {}
    import c._

    def connectionCountEquals(sr: InMemoryStatsReceiver, num: Float) {
      assert(sr.gauges(Seq("connections"))() == num)
    }

    connectionCountEquals(sr, 0)

    handler.channelActive(ctx)
    connectionCountEquals(sr, 1)

    handler.channelActive(ctx)
    connectionCountEquals(sr, 2)

    handler.channelInactive(ctx)
    connectionCountEquals(sr, 1)

    handler.channelInactive(ctx)
    connectionCountEquals(sr, 0)
  }

  def channelLifeCycleTest(
    counterName: String,
    f: (ChannelDuplexHandler, ChannelHandlerContext) => Unit
  ) = test(s"ChannelStatsHandler counts $counterName") {
    val tc = new TestContext {}
    import tc._

    assert(!sr.counters.contains(Seq(counterName)))
    f(handler, ctx)
    assert(sr.counters(Seq(counterName)) == 1)
  }

  channelLifeCycleTest(
    "closes",
    (handler, ctx) => handler.close(ctx, mock[ChannelPromise])
  )

  channelLifeCycleTest(
    "closechans",
    (handler, ctx) => handler.channelInactive(ctx)
  )

  channelLifeCycleTest(
    "connects",
    (handler, ctx) => handler.channelActive(ctx)
  )

  test("ChannelStatsHandler records connection duration") {
    Time.withCurrentTimeFrozen { control =>
      val tc = new TestContext { }
      import tc._
      handler.channelActive(ctx)
      control.advance(Duration.fromMilliseconds(100))
      handler.channelInactive(ctx)
      assert(sr.stat("connection_duration")() == Seq(100.0))
      handler.channelActive(ctx)
      control.advance(Duration.fromMilliseconds(200))
      handler.channelInactive(ctx)
      assert(sr.stat("connection_duration")() == Seq(100.0, 300.0))
    }
  }

  test("ChannelStatsHandler counts exceptions") {
    val tc = new TestContext { }
    import tc._

    handler.exceptionCaught(ctx, new RuntimeException)
    handler.exceptionCaught(ctx, new TimeoutException)
    handler.exceptionCaught(ctx, new Exception)
    assert(sr.counters(Seq("exn", "java.lang.RuntimeException")) == 1)
    assert(sr.counters(Seq("exn", "java.lang.Exception")) == 1)
    assert(sr.counters(Seq("exn", "java.util.concurrent.TimeoutException")) == 1)
  }

  test("ChannelStatsHandler counts sent and received bytes") {
    val tc = new TestContext { }
    import tc._

    handler.channelActive(ctx)
    handler.write(ctx, wrappedBuffer(Array.fill(42)(0.toByte)), mock[ChannelPromise])
    handler.channelInactive(ctx)

    assert(sr.counter("sent_bytes")() == 42)
    assert(bytesWritten.get == 42)
    assert(sr.stat("connection_received_bytes")() == Seq(0.0))
    assert(sr.stat("connection_sent_bytes")() == Seq(42.0))

    resetConnection()

    handler.channelActive(ctx)
    handler.channelRead(ctx, wrappedBuffer(Array.fill(123)(0.toByte)))
    handler.channelInactive(ctx)

    assert(sr.counter("received_bytes")() == 123)
    assert(bytesReceived.get == 123)
    assert(sr.stat("connection_received_bytes")() == Seq(0.0, 123.0))
    assert(sr.stat("connection_sent_bytes")() == Seq(42.0, 0.0))
  }
}
