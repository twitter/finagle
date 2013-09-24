package com.twitter.finagle.channel

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.TimeConversions.intToTimeableNumber
import com.twitter.util.{Duration, Time}
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
    val al = new AtomicLong()
    val handler = new ChannelStatsHandler(NullStatsReceiver, al)
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
