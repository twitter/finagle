package com.twitter.finagle.channel

import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.util.TimeConversions.intToTimeableNumber
import com.twitter.util.{Duration, Promise, Time}
import java.util.concurrent.atomic.AtomicLong
import org.jboss.netty.channel._
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ChannelStatsHandlerTest extends FunSpec with MockitoSugar {
  trait TimeUnwritableInterestChangedTest {
    val handler = new ChannelStatsHandler(NullStatsReceiver)
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
  describe("ChannelStatsHandler") {

    it("should count connections") {
      val sr = new InMemoryStatsReceiver()

      def connectionsIs(num: Int) {
        assert(sr.gauges(Seq("connections"))() === num)
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
      p.setValue(())

      connectionsIs(0)
    }

    it("should detect how long the channel has been writable") {
      val time = Time.now
      Time.withTimeFunction(time) { control =>
        new TimeUnwritableInterestChangedTest {
          control.advance(1.minute)
          handler.channelInterestChanged(ctxWritable, e)
          assert(handler.writableDuration === 1.minute)
          assert(handler.unwritableDuration === Duration.Zero)
        }
      }
    }

    it("should detect how long the channel has been unwritable") {
      val time = Time.now
      Time.withTimeFunction(time) { control =>
        new TimeUnwritableInterestChangedTest {
          control.advance(1.minute)
          handler.channelInterestChanged(ctxUnwritable, e)
          control.advance(1.minute)
          assert(handler.writableDuration === Duration.Zero)
          assert(handler.unwritableDuration === 1.minute)
        }
      }
    }

    it("should detect how long the channel has been writable after having switched back") {
      val time = Time.now
      Time.withTimeFunction(time) { control =>
        new TimeUnwritableInterestChangedTest {
          control.advance(1.minute)
          handler.channelInterestChanged(ctxUnwritable, e)
          control.advance(1.minute)
          handler.channelInterestChanged(ctxWritable, e)
          control.advance(1.minute)
          assert(handler.writableDuration === 1.minute)
          assert(handler.unwritableDuration === Duration.Zero)
        }
      }
    }
  }
}
