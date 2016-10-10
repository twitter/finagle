package com.twitter.finagle.thrift.transport.netty3

import com.twitter.finagle.thrift.Protocols
import com.twitter.finagle.thrift.transport.AbstractBufferedTransportDecoderTest
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder

class ThriftBufferedTransportDecoderTest extends AbstractBufferedTransportDecoderTest {

  private def getDecoder = new ThriftBufferedTransportDecoder(Protocols.factory())

  override def decode(arrays: Seq[Array[Byte]]): Vector[Array[Byte]] = {
    val channel = new DecoderEmbedder[ChannelBuffer](getDecoder)

    arrays.foreach { data => channel.offer(ChannelBuffers.wrappedBuffer(data)) }

    channel.pollAll().map {
      case buf: ChannelBuffer =>
        val data = new Array[Byte](buf.readableBytes())
        buf.readBytes(data)
        assert(buf.readableBytes() == 0)
        data

      case other => sys.error(s"Unexpected output: ${other.getClass.getSimpleName}: $other")
    }.toVector
  }
}
