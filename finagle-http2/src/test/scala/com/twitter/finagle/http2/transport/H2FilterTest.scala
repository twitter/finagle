package com.twitter.finagle.http2.transport

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{MockTimer, Time}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.{DefaultHttp2DataFrame, Http2FrameCodecBuilder}
import org.scalatest.FunSuite

class H2FilterTest extends FunSuite {
  test("doesn't leak messages") {
    val em = new EmbeddedChannel(new H2Filter(DefaultTimer))
    val payload = io.netty.buffer.Unpooled.buffer(10)
    assert(payload.refCnt() == 1)
    em.writeInbound(new DefaultHttp2DataFrame(payload))
    assert(payload.refCnt() == 0)
  }

  private class Ctx {
    val timer = new MockTimer
    val em = new EmbeddedChannel()
    val filter = new H2Filter(timer)
    val h2FrameCodec = Http2FrameCodecBuilder.forServer().build()

    em.pipeline.addFirst(H2Filter.HandlerName, filter)
    em.pipeline.addFirst("TheFrameCodec", h2FrameCodec)
  }

  test("If deadline is not set we do a normal close") {
    new Ctx {
      val channelFuture = em.close()
      assert(channelFuture.isSuccess)
    }
  }

  test("If deadline was set we defer shutdown")(new Ctx {
    Time.withCurrentTimeFrozen { timeControl =>
      filter.setDeadline(Time.now + 5.seconds)
      // Just make sure our test is sane.
      assert(h2FrameCodec.connection.remote.lastStreamKnownByPeer != Int.MaxValue)

      val closeF = em.close()
      assert(!closeF.isDone)
      assert(h2FrameCodec.connection.goAwaySent)
      assert(h2FrameCodec.connection.remote.lastStreamKnownByPeer == Int.MaxValue)

      // Make sure that if the deadline expires before we get closed on we force close.
      timeControl.advance(5.seconds)
      timer.tick()
      em.runPendingTasks()
      assert(closeF.isSuccess)
      // The second GOAWAY should say what the last truly handled stream was, which
      // was 0 since there were in fact no streams at all.
      assert(h2FrameCodec.connection.remote.lastStreamKnownByPeer == 0)
    }
  })
}
