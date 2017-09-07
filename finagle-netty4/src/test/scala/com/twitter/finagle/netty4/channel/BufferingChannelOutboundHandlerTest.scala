package com.twitter.finagle.netty4.channel

import io.netty.channel.{ChannelHandlerContext, ChannelOutboundHandlerAdapter}
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.runner.RunWith
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BufferingChannelOutboundHandlerTest
    extends FunSuite
    with GeneratorDrivenPropertyChecks
    with OneInstancePerTest {

  class Buffering extends ChannelOutboundHandlerAdapter with BufferingChannelOutboundHandler {
    private[this] var ctx: ChannelHandlerContext = _

    override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
      this.ctx = ctx
    }

    def writeAll(): Unit = {
      writePendingWritesAndFlushIfNeeded(ctx)
    }

    def failAll(cause: Throwable): Unit = {
      failPendingWrites(cause)
    }
  }

  val handler = new Buffering
  val channel = new EmbeddedChannel(handler)

  test("writeAll") {
    forAll { s: String =>
      channel.writeOutbound(s)
      assert(channel.outboundMessages().size() == 0)

      handler.writeAll()
      assert(channel.readOutbound[String]() == s)
    }

    channel.finishAndReleaseAll()
  }

  test("failAll") {
    forAll { s: String =>
      channel.writeOutbound(s)
      assert(channel.outboundMessages().size() == 0)
      val failure = new Exception("not good at all")

      handler.failAll(failure)
      assert(intercept[Exception](channel.checkException()) == failure)
    }

    // We should be able to write afterwards.
    channel.writeOutbound("pending", "write")
    assert(channel.outboundMessages().size() == 0)
    handler.writeAll()
    assert(channel.readOutbound[String]() == "pending")
    assert(channel.readOutbound[String]() == "write")

    channel.finishAndReleaseAll()
  }
}
