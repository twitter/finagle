package com.twitter.finagle.thrift.transport.netty4

import com.twitter.finagle.thrift.Protocols
import com.twitter.finagle.thrift.transport.AbstractBufferedTransportDecoderTest
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel

class ThriftBufferedTransportDecoderTest extends AbstractBufferedTransportDecoderTest {

  private def getArray(buf: ByteBuf): Array[Byte] = {
    val out = new Array[Byte](buf.readableBytes())
    buf.readBytes(out)
    assert(buf.readableBytes() == 0)
    out
  }
  private def getDecoder = new ThriftBufferedTransportDecoder(Protocols.factory())

  def decode(arrays: Seq[Array[Byte]]): Vector[Array[Byte]] = {
    val data = arrays.map(Unpooled.wrappedBuffer(_))
    val channel = new EmbeddedChannel()

    channel.pipeline().addLast(getDecoder)
    channel.writeInbound(data: _*)

    var acc = Vector.empty[Array[Byte]]
    while (!channel.inboundMessages().isEmpty) {
      acc :+= getArray(channel.readInbound[ByteBuf]())
    }

    acc
  }
}
