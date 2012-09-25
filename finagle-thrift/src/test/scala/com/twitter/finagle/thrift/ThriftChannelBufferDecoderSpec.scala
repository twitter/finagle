package com.twitter.finagle.thrift

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{ChannelHandlerContext, Channel}

class ThriftChannelBufferDecoderSpec extends SpecificationWithJUnit with Mockito {
  "ThriftChannelBufferDecoder" should {
    val decoder = new ThriftChannelBufferDecoder
    val ctx = mock[ChannelHandlerContext]
    val ch = mock[Channel]

    "convert channel buffers to arrays directly" in {
      val arr = "hello, world!".getBytes
      val buf = ChannelBuffers.wrappedBuffer(arr)
      decoder.decode(ctx, ch, buf) must beLike {
        case a: Array[Byte] => arr eq a
      }
    }

    "convert channel buffers to arrays with offset" in {
      val arr = "hello, world!".getBytes
      val buf = ChannelBuffers.wrappedBuffer(arr)
      buf.readByte()
      decoder.decode(ctx, ch, buf) must beLike {
        case a: Array[Byte] => (arr drop 1).toSeq == a.toSeq
      }
    }

    "convert composite buffers to arrays" in {
      val arr = "hello, world!".getBytes
      val buf = ChannelBuffers.wrappedBuffer(arr take 2, arr drop 2)
      decoder.decode(ctx, ch, buf) must beLike {
        case a: Array[Byte] => arr.toSeq == a.toSeq
      }
    }

    "fail to convert non buffers" in {
      decoder.decode(ctx, ch, new {}) must throwA(
        new IllegalArgumentException("no byte buffer"))
    }
  }
}
