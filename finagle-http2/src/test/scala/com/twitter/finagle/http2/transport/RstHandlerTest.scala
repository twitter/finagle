package com.twitter.finagle.http2.transport

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame
import org.scalatest.FunSuite

class RstHandlerTest extends FunSuite {
  test("doesn't leak message written post-RST") {
    val em = new EmbeddedChannel(new RstHandler)
    em.writeInbound(new DefaultHttp2ResetFrame(404))
    val msg = io.netty.buffer.Unpooled.buffer(10)
    assert(msg.refCnt() == 1)
    em.writeOneOutbound(msg)
    assert(msg.refCnt() == 0)
  }
}
