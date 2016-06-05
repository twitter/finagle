package com.twitter.finagle.netty4.http

import com.twitter.conversions.storage._
import com.twitter.finagle.ChannelBufferUsageException
import com.twitter.finagle.http.codec.ChannelBufferUsageTracker
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelPromise, ChannelHandlerContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ByteBufManagerTest extends FunSuite with MockitoSugar {

  def usageTrackerFactory() = {
    val usageTracker = new ChannelBufferUsageTracker(1000.bytes)
    assert(usageTracker.usageLimit == (1000.bytes))
    usageTracker
  }

  val ctx = mock[ChannelHandlerContext]
  val p = mock[ChannelPromise]

  test("tracks buffer usage between writes") {
    val tracker = usageTrackerFactory()
    val handler = new ByteBufManager(tracker)

    handler.channelRead(ctx, Unpooled.buffer(100))
    assert(tracker.currentUsage == 100.bytes)
    handler.channelRead(ctx, Unpooled.buffer(50))
    assert(tracker.currentUsage == 150.bytes)

    handler.write(ctx, Unpooled.EMPTY_BUFFER, p)
    assert(tracker.currentUsage == 0.bytes)

    handler.channelRead(ctx, Unpooled.buffer(123))
    assert(tracker.currentUsage == 123.bytes)
    handler.close(ctx, p)
    assert(tracker.currentUsage == 0.bytes)
  }

  test("throws if aggregate limit is exceeded") {
    val tracker = usageTrackerFactory()
    val handler = new ByteBufManager(tracker)

    handler.channelRead(ctx, Unpooled.buffer(1000))

    intercept[ChannelBufferUsageException] {
      handler.channelRead(ctx, Unpooled.buffer(1))
    }
  }
}
