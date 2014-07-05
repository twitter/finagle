package com.twitter.finagle.thrift

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{ChannelHandlerContext, Channel}
import org.junit.runner.RunWith
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ThriftChannelBufferDecoderSpec extends FunSuite with MockitoSugar with OneInstancePerTest{
    val decoder = new ThriftChannelBufferDecoder
    val ctx = mock[ChannelHandlerContext]
    val ch = mock[Channel]

    test("ThriftChannelBufferDecoder convert channel buffers to arrays directly") {
      val arr = "hello, world!".getBytes
      val buf = ChannelBuffers.wrappedBuffer(arr)
      decoder.decode(ctx, ch, buf) match {
        case a: Array[Byte] => assert(arr === a)
        case _ => fail()
      }
    }

    test("ThriftChannelBufferDecoder convert channel buffers to arrays with offset") {
      val arr = "hello, world!".getBytes
      val buf = ChannelBuffers.wrappedBuffer(arr)
      buf.readByte()
      decoder.decode(ctx, ch, buf) match {
        case a: Array[Byte] => assert((arr drop 1).toSeq === a.toSeq)
        case _ => fail()
      }
    }

    test("ThriftChannelBufferDecoder convert composite buffers to arrays"){
      val arr = "hello, world!".getBytes
      val buf = ChannelBuffers.wrappedBuffer(arr take 2, arr drop 2)
      decoder.decode(ctx, ch, buf) match {
        case a: Array[Byte] => assert(arr.toSeq === a.toSeq)
      }
    }

    test("ThriftChannelBufferDecoder fail to convert non buffers") {
      intercept[IllegalArgumentException]{
        decoder.decode(ctx, ch, new {})
      }
    }
}
