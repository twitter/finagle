package com.twitter.finagle.exp.mysql.transport

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class PacketFrameDecoderTest extends FunSuite with MockitoSugar {
  val ctx = mock[ChannelHandlerContext]
  val c = mock[Channel]
  val frameDecoder = new PacketFrameDecoder

  test("ignore incomplete packets") {
    val partial = Array[Byte](0x05, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03)
    val result = frameDecoder.decode(ctx, c, ChannelBuffers.wrappedBuffer(partial))
    assert(result === null)
  }

  test("decode complete packets") {
    val complete = Array[Byte](0x02, 0x00, 0x00, 0x01, 0x01, 0x02)
    val result = frameDecoder.decode(ctx, c, ChannelBuffers.wrappedBuffer(complete))
    assert(result != null)
    assert(result.size == 2)
    assert(result.seq == 1)
    assert(result.body.underlying.array === Array[Byte](0x01, 0x02))
  }

  test("16Mbyte packets") {
    val ff = -1.toByte
    val frame: Array[Byte] = Array[Byte](ff, ff, ff, 0x01) ++ Array.fill[Byte](0xffffff)(0x00)
    val result = frameDecoder.decode(ctx, c, ChannelBuffers.wrappedBuffer(frame))
    assert(result != null)
    assert(result.size == 16777215)
    assert(result.seq == 1)
  }
}
