package com.twitter.finagle.thrift

import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.replay.{ReplayingDecoder, VoidEnum}

import org.apache.thrift.protocol.{TProtocolFactory, TProtocolUtil, TType}

class ThriftBufferCodec(protocolFactory: TProtocolFactory)
  extends ReplayingDecoder[VoidEnum]
{
  override def decode(
    ctx: ChannelHandlerContext, channel: Channel,
    buffer: ChannelBuffer, state: VoidEnum
  ) = {
    val transport = new ChannelBufferToTransport(buffer)
    val iprot = protocolFactory.getProtocol(transport)

    val beginIndex = buffer.readerIndex
    buffer.markReaderIndex()

    iprot.readMessageBegin()
    TProtocolUtil.skip(iprot, TType.STRUCT)
    iprot.readMessageEnd()    

    val endIndex = buffer.readerIndex
    buffer.resetReaderIndex()

    buffer.readSlice(endIndex - beginIndex)
  }
}
