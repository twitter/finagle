package com.twitter.finagle.http2.transport

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.DefaultHttp2DataFrame
import org.scalatest.FunSuite

class H2FilterTest extends FunSuite {
  test("doesn't leak messages") {
    val em = new EmbeddedChannel(H2Filter)
    val payload = io.netty.buffer.Unpooled.buffer(10)
    assert(payload.refCnt() == 1)
    em.writeInbound(new DefaultHttp2DataFrame(payload))
    assert(payload.refCnt() == 0)
  }
}
