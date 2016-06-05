package com.twitter.finagle.spdy

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.embedder.{DecoderEmbedder, EncoderEmbedder}
import org.jboss.netty.handler.codec.spdy.{DefaultSpdyHeadersFrame, SpdyHeadersFrame, SpdyVersion}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpdyRawFrameCodecTest extends FunSuite {

  val headersFrame: Array[Byte] = Array[Int](
    0x80, 0x03, 0x00, 0x08, 0x00, 0x00, 0x00, 0x19, // SPDY/3.1 Headers Frame
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, // Stream-ID 1, 1 Name/Value Pair
    0x00, 0x00, 0x00, 0x04, 0x6E, 0x61, 0x6D, 0x65, // (4) name
    0x00, 0x00, 0x00, 0x05, 0x76, 0x61, 0x6C, 0x75, // (5) value
    0x65
  ).map { _.toByte }

  def spdyFrameCodec = new SpdyRawFrameCodec(SpdyVersion.SPDY_3_1, 8192, 16384)

  test("decode") {
    val channel = new DecoderEmbedder(spdyFrameCodec)
    channel.offer(ChannelBuffers.wrappedBuffer(headersFrame))
    val spdyHeadersFrame = channel.poll().asInstanceOf[SpdyHeadersFrame]
    assert(spdyHeadersFrame.headers.entries.size == 1)
    assert(spdyHeadersFrame.headers.entries.get(0).getKey == "name")
    assert(spdyHeadersFrame.headers.entries.get(0).getValue == "value")
    assert(channel.finish == false)
  }

  test("encode") {
    val channel = new EncoderEmbedder(spdyFrameCodec)
    val spdyHeadersFrame = new DefaultSpdyHeadersFrame(1)
    spdyHeadersFrame.headers.add("name", "value")
    channel.offer(spdyHeadersFrame)
    val channelBuffer = channel.poll().asInstanceOf[ChannelBuffer]
    assert(channelBuffer.readableBytes == headersFrame.length)
    headersFrame foreach { b => assert(channelBuffer.readByte == b) }
    assert(channel.finish == false)
  }
}
