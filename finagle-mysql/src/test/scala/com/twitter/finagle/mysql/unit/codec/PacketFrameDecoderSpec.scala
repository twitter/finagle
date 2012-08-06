package com.twitter.finagle.mysql.codec

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.finagle.mysql.protocol.{Buffer, Packet}
import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffers._

class PacketFrameDecoderSpec extends SpecificationWithJUnit with Mockito {
  val ctx = mock[ChannelHandlerContext]
  val c = mock[Channel]

  def makeBuffer(packet: Packet) = {
    val header = packet.header.toChannelBuffer
    val body = Buffer.toChannelBuffer(packet.body)
    wrappedBuffer(header, body)
  }

  "PacketFrameDecoder" should {
    val frameDecoder = new PacketFrameDecoder
    val partial = Packet(5, 0, Array[Byte](0x00, 0x01))

    "ignore incomplete packets" in {
      frameDecoder.decode(ctx, c, makeBuffer(partial)) must beNull
    }

    val complete = Packet(5, 0, Array[Byte](0x01, 0x01, 0x02, 0x03, 0x04))

    "decode complete packets" in {
      val result = frameDecoder.decode(ctx, c, makeBuffer(complete))
      result.header.size mustEqual complete.header.size
      result.header.seq mustEqual complete.header.seq
      result.body.toList must containAll(complete.body.toList)
    }
  }
}