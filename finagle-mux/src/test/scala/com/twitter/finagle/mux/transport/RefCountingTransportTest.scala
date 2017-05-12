package com.twitter.finagle.mux.transport

import com.twitter.conversions.time._
import com.twitter.finagle.Status
import com.twitter.finagle.netty4.ByteBufAsBuf
import com.twitter.io.Buf
import com.twitter.util.Await
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import java.nio.charset.StandardCharsets.UTF_8
import org.scalatest.FunSuite

class RefCountingTransportTest extends FunSuite {
  test("releaseFn sees and transforms buffered messages on session close") {
    val nettyBuf = Unpooled.directBuffer(3).writeBytes("abc".getBytes(UTF_8))

    val em = new EmbeddedChannel
    val ct = new RefCountingTransport(em)

    em.writeInbound(new ByteBufAsBuf(nettyBuf))
    assert(nettyBuf.refCnt() == 1)

    Await.ready(ct.close(), 1.second)
    // after close the buffer is copied + released

    val Buf.Utf8(res) = Await.result(ct.read(), 1.second)
    assert(res == "abc")
    assert(nettyBuf.refCnt() == 0)
  }

  test("releaseFn sees failed offers") {
    val em = new EmbeddedChannel
    val ct = new RefCountingTransport(em, maxPendingOffers = 1)

    val fillerBuf = Unpooled.directBuffer(10).writeBytes("full".getBytes(UTF_8))
    em.writeInbound(new ByteBufAsBuf(fillerBuf)) // backing async queue is now full

    val doomedBuf = Unpooled.directBuffer(10).writeBytes("doomed".getBytes(UTF_8))
    em.writeInbound(new ByteBufAsBuf(doomedBuf))

    // channel transport is consequently failed
    assert(ct.status == Status.Closed)
    assert(fillerBuf.refCnt() == 0)
    assert(doomedBuf.refCnt() == 0)

    // original buffered message isn't discarded
    val Buf.Utf8(res) = Await.result(ct.read(), 1.second)
    assert(res == "full")
  }
}
