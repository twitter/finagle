package com.twitter.finagle.http2.transport.server

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{MockTimer, Time}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.{DefaultHttp2DataFrame, Http2FrameCodecBuilder}
import org.scalatest.funsuite.AnyFunSuite

class H2ServerFilterTest extends AnyFunSuite {

  private class Ctx {
    val timer = new MockTimer
    val em = new EmbeddedChannel()
    val filter = new H2ServerFilter(timer, em)
    val h2FrameCodec = Http2FrameCodecBuilder.forServer().build()

    init()

    def init(): Unit = {
      em.pipeline.addLast("TheFrameCodec", h2FrameCodec)
      em.pipeline.addLast(H2ServerFilter.HandlerName, filter)
    }
  }

  test("doesn't leak messages") {
    val em = new EmbeddedChannel()
    em.pipeline.addLast(new H2ServerFilter(DefaultTimer, em))
    val payload = io.netty.buffer.Unpooled.buffer(10)
    assert(payload.refCnt() == 1)
    em.writeInbound(new DefaultHttp2DataFrame(payload))
    assert(payload.refCnt() == 0)
  }

  test("a pipeline exception results in shutdown")(new Ctx {
    em.pipeline.fireExceptionCaught(new Exception)
    assert(em.closeFuture.isSuccess)
  })

  test("Channel close calls are swallowed by the filter until a normal shutdown") {
    new Ctx {
      val channelFuture = em.close()
      assert(!channelFuture.isDone)
      filter.gracefulShutdown(Time.Bottom)
      assert(channelFuture.isSuccess)
    }
  }

  test("If deadline was set we defer shutdown")(new Ctx {
    Time.withCurrentTimeFrozen { timeControl =>
      assert(h2FrameCodec.connection.remote.lastStreamKnownByPeer != Int.MaxValue)
      filter.gracefulShutdown(Time.now + 5.seconds)
      em.runPendingTasks()

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

  test("initiates close if the close deadline channel attribute has been set") {
    Time.withCurrentTimeFrozen { timeControl =>
      new Ctx {
        override def init(): Unit = {
          em.attr(H2ServerFilter.CloseRequestAttribute).set(Time.now + 5.seconds)
          super.init()
        }

        assert(em.isOpen)
        assert(h2FrameCodec.connection.goAwaySent)

        timeControl.advance(5.seconds)
        timer.tick()
        em.runPendingTasks()
        assert(!em.isOpen)
      }
    }
  }

  test("doesn't initiate close if the close deadline channel attribute hasn't been set") {
    Time.withCurrentTimeFrozen { _ =>
      new Ctx {
        assert(em.isOpen)
        assert(!h2FrameCodec.connection.goAwaySent)
      }
    }
  }
}
