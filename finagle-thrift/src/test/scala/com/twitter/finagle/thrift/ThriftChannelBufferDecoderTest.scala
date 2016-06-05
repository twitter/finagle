package com.twitter.finagle.thrift

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{ChannelHandlerContext, Channel}
import org.junit.runner.RunWith
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ThriftChannelBufferDecoderTest extends FunSuite with MockitoSugar {

  class ThriftChannelBufferDecoderContext{
    val decoder: ThriftChannelBufferDecoder = new ThriftChannelBufferDecoder
    val ctx: ChannelHandlerContext = mock[ChannelHandlerContext]
    val ch: Channel = mock[Channel]
  }

  test("ThriftChannelBufferDecoder convert channel buffers to arrays directly") {
    val c = new ThriftChannelBufferDecoderContext
    import c._

    val arr = "hello, world!".getBytes
    val buf = ChannelBuffers.wrappedBuffer(arr)
    decoder.decode(ctx, ch, buf) match {
      case a: Array[Byte] => assert(a == arr)
      case _ => fail()
    }
  }

  test("ThriftChannelBufferDecoder convert channel buffers to arrays with offset") {
    val c = new ThriftChannelBufferDecoderContext
    import c._

    val arr = "hello, world!".getBytes
    val buf = ChannelBuffers.wrappedBuffer(arr)
    buf.readByte()
    decoder.decode(ctx, ch, buf) match {
      case a: Array[Byte] => assert(a.toSeq == (arr drop 1).toSeq)
      case _ => fail()
    }
  }

  test("ThriftChannelBufferDecoder convert composite buffers to arrays") {
    val c = new ThriftChannelBufferDecoderContext
    import c._

    val arr = "hello, world!".getBytes
    val buf = ChannelBuffers.wrappedBuffer(arr take 2, arr drop 2)
    decoder.decode(ctx, ch, buf) match {
      case a: Array[Byte] => assert(a.toSeq == arr.toSeq)
    }
  }

  test("ThriftChannelBufferDecoder fail to convert non buffers") {
    val c = new ThriftChannelBufferDecoderContext
    import c._

    intercept[IllegalArgumentException] {
      decoder.decode(ctx, ch, new {})
    }
  }
}
