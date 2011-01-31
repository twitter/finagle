package com.twitter.finagle.thrift

import org.jboss.netty.channel.{
  SimpleChannelHandler, Channel, ChannelEvent, ChannelHandlerContext,
  SimpleChannelDownstreamHandler, MessageEvent, Channels,
  ChannelPipelineFactory}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder

import com.twitter.finagle.Codec

class ThriftServerChannelBufferEncoder extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      // An empty array indicates a oneway reply.
      case array: Array[Byte] if (!array.isEmpty) =>
        val buffer = ChannelBuffers.wrappedBuffer(array)
        Channels.write(ctx, e.getFuture, buffer)
      case array: Array[Byte] => ()
      case _ => throw new IllegalArgumentException("no byte array")
    }
  }
}

object ThriftServerFramedCodec {
  def apply() = new ThriftServerFramedCodec
}

class ThriftServerFramedCodec extends Codec[Array[Byte], Array[Byte]] {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder", new ThriftServerChannelBufferEncoder)
        pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
        pipeline
      }
    }

  val serverPipelineFactory = clientPipelineFactory
}
