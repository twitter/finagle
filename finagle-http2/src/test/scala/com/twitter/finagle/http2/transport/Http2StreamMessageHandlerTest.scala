package com.twitter.finagle.http2.transport

import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.{
  DefaultHttp2DataFrame,
  DefaultHttp2Headers,
  DefaultHttp2HeadersFrame,
  DefaultHttp2ResetFrame,
  DefaultHttp2WindowUpdateFrame
}
import io.netty.util.ReferenceCounted
import org.scalatest.FunSuite

class Http2StreamMessageHandlerTest extends FunSuite {
  test("doesn't leak message written post-RST") {
    val em = new EmbeddedChannel(new Http2StreamMessageHandler)
    em.writeInbound(new DefaultHttp2ResetFrame(404))
    val msg = io.netty.buffer.Unpooled.buffer(10)
    assert(msg.refCnt() == 1)
    em.writeOneOutbound(msg)
    assert(msg.refCnt() == 0)
  }

  test("Strips Http2WindowUpdate frames") {
    val em = new EmbeddedChannel(new Http2StreamMessageHandler)
    em.writeInbound(new DefaultHttp2WindowUpdateFrame(1))
    assert(em.readInbound[Object]() == null)
  }

  test("Propagates an IllegalArgumentException for unknown frame types") {
    val badFrames = Seq(
      new DefaultHttp2HeadersFrame(new DefaultHttp2Headers),
      new DefaultHttp2DataFrame(Unpooled.directBuffer(10)),
      new Object
    )

    badFrames.foreach { badFrame =>
      val em = new EmbeddedChannel(new Http2StreamMessageHandler)
      intercept[IllegalArgumentException] {
        em.writeInbound(badFrame)
      }

      badFrame match {
        case r: ReferenceCounted => assert(r.refCnt == 0)
        case _ => ()
      }
    }
  }
}
