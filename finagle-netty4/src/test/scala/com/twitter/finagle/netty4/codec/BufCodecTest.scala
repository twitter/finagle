package com.twitter.finagle.netty4.codec

import com.twitter.finagle.Failure
import com.twitter.io.{Buf, Charsets}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BufCodecTest extends FunSuite {
  test("decode") {
    val ch = new EmbeddedChannel(new BufCodec)
    val q = ch.inboundMessages

    ch.writeInbound(Unpooled.wrappedBuffer("hello".getBytes(Charsets.Utf8)))
    assert(q.size == 1)
    assert(q.poll() == Buf.Utf8("hello"))
    assert(q.size == 0)

    intercept[Failure] { ch.writeInbound(new Object) }
  }

  test("encode") {
    val ch = new EmbeddedChannel(new BufCodec)
    val q = ch.outboundMessages

    ch.writeOutbound(Buf.Utf8("hello"))
    assert(q.size == 1)
    assert(q.peek().isInstanceOf[ByteBuf])
    val bb = q.poll().asInstanceOf[ByteBuf]
    assert(bb.toString(Charsets.Utf8) == "hello")
    assert(q.size == 0)

    val channelFuture = ch.write(new Object)
    assert(channelFuture.cause.isInstanceOf[Failure])
  }
}