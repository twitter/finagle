package com.twitter.finagle.netty4.channel

import com.twitter.finagle.netty4.channel.ChannelRequestStatsHandler.SharedChannelRequestStats
import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.channel._
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ChannelRequestStatsHandlerTest extends AnyFunSuite with MockitoSugar {

  def requestsEqual(sr: InMemoryStatsReceiver, requests: Seq[Float]): Unit =
    assert(
      sr.stat("connection_requests")() == requests
    )

  private class TestContext(sharedStats: SharedChannelRequestStats) {
    val handler = new ChannelRequestStatsHandler(sharedStats)

    val ctx = mock[ChannelHandlerContext]
    val chan = mock[Channel]
    when(ctx.channel).thenReturn(chan)

    val msg = new Object
  }

  test("ChannelRequestStatsHandler counts messages") {
    val sr = new InMemoryStatsReceiver
    requestsEqual(sr, Seq.empty[Float])

    val testCtx = new TestContext(new SharedChannelRequestStats(sr))
    val handler = testCtx.handler
    val ctx = testCtx.ctx
    val msg = testCtx.msg

    // first connection sends two messages
    handler.handlerAdded(ctx)
    handler.channelRead(ctx, msg)
    handler.channelRead(ctx, msg)
    handler.channelInactive(ctx)

    // second connection sends zero
    handler.handlerAdded(ctx)
    handler.channelInactive(ctx)

    // third connection sends one
    handler.handlerAdded(ctx)
    handler.channelRead(ctx, msg)
    handler.channelInactive(ctx)

    requestsEqual(sr, Seq(2.0f, 0.0f, 1.0f))
  }

  test("ChannelRequestStatsHandler handles multiple channelInactive calls") {
    val sr = new InMemoryStatsReceiver
    requestsEqual(sr, Seq.empty[Float])

    val testCtx = new TestContext(new SharedChannelRequestStats(sr))
    val handler = testCtx.handler
    val ctx = testCtx.ctx
    val msg = testCtx.msg

    handler.handlerAdded(ctx)
    handler.channelRead(ctx, msg)
    requestsEqual(sr, Seq())

    handler.channelInactive(ctx)
    requestsEqual(sr, Seq(1.0f))

    handler.channelInactive(ctx)
    requestsEqual(sr, Seq(1.0f))
  }

  test("SharedChannelRequestStats collects all handlers' connection requests") {
    val sr = new InMemoryStatsReceiver
    requestsEqual(sr, Seq.empty[Float])

    val testCtx1 = new TestContext(new SharedChannelRequestStats(sr))
    val testCtx2 = new TestContext(new SharedChannelRequestStats(sr))
    val ctx1 = testCtx1.ctx
    val ctx2 = testCtx2.ctx
    val handler1 = testCtx1.handler
    val handler2 = testCtx2.handler
    val msg1 = testCtx1.msg
    val msg2 = testCtx2.msg

    handler1.handlerAdded(ctx1)
    handler1.channelRead(ctx1, msg1)
    handler1.channelRead(ctx1, msg1)
    handler2.handlerAdded(ctx2)
    handler2.channelRead(ctx2, msg2)
    requestsEqual(sr, Seq())

    handler1.channelInactive(ctx1)
    requestsEqual(sr, Seq(2.0f))

    handler2.channelInactive(ctx2)
    requestsEqual(sr, Seq(2.0f, 1.0f))
  }
}
