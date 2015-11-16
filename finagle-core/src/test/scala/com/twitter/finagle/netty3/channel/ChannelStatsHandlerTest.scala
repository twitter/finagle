package com.twitter.finagle.netty3.channel

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.TimeConversions.intToTimeableNumber
import com.twitter.util.{Promise, Time}
import java.util.concurrent.atomic.AtomicLong
import org.jboss.netty.channel._
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ChannelStatsHandlerTest extends FunSpec with MockitoSugar {
  trait SocketTest {
    val e = mock[ChannelStateEvent]
    val chanWritable = mock[Channel]
    val chanUnwritable = mock[Channel]
    val ctxWritable = mock[ChannelHandlerContext]
    val ctxUnwritable = mock[ChannelHandlerContext]
    when(chanWritable.isWritable).thenReturn(true)
    when(chanUnwritable.isWritable).thenReturn(false)
    when(ctxWritable.getChannel()).thenReturn(chanWritable)
    when(ctxUnwritable.getChannel()).thenReturn(chanUnwritable)
  }

  trait InMemoryStatsTest extends SocketTest {
    val sr = new InMemoryStatsReceiver()
    val handler = new ChannelStatsHandler(sr)
  }

  describe("ChannelStatsHandler") {
    it("should count connections") {
      val sr = new InMemoryStatsReceiver()

      def connectionsIs(num: Int) {
        assert(sr.gauges(Seq("connections"))() == num)
      }

      val handler = new ChannelStatsHandler(sr)
      connectionsIs(0)

      val p = Promise[Unit]()
      val ctx = mock[ChannelHandlerContext]
      val al = new AtomicLong()
      val obj = (al, al).asInstanceOf[Object]
      when(ctx.getAttachment()).thenReturn(obj, obj)
      handler.channelConnected(ctx, p)

      connectionsIs(1)
      p.setDone()

      connectionsIs(0)
    }

    it("should check the counters are collected correctly") {
      val time = Time.now
      Time.withTimeFunction(time) { control =>
        new InMemoryStatsTest {
          control.advance(5.minutes)
          handler.channelInterestChanged(ctxUnwritable, e)
          control.advance(10.minutes)
          handler.channelInterestChanged(ctxWritable, e)
          control.advance(20.minutes)
          handler.channelInterestChanged(ctxUnwritable, e)
          assert(sr.counters(Seq("socket_writable_ms")) == 25.minutes.inMillis)
          assert(sr.counters(Seq("socket_unwritable_ms")) == 10.minutes.inMillis)
        }
      }
    }

    it("should count written bytes") {
      val sr = new InMemoryStatsReceiver

      val handler = new ChannelStatsHandler(sr)

      val ctx = mock[ChannelHandlerContext]
      val al = new AtomicLong

      val counters = (al, al).asInstanceOf[Object]
      when(ctx.getAttachment()).thenReturn(counters, counters)

      val evt = mock[WriteCompletionEvent]
      when(evt.getWrittenAmount).thenReturn(42)

      handler.writeComplete(ctx, evt)

      assert(sr.counter("sent_bytes")() == 42)
      assert(al.get == 42)
    }
  }
}
