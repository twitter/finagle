package com.twitter.finagle.thrift

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.buffer.ChannelBuffer

import com.twitter.finagle.builder.Codec

class Thrift extends Codec[Array[Byte], Array[Byte]] {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder", new ChannelBufferEncoder)
        pipeline.addLast("byteDecoder", new ChannelBufferDecoder)
        pipeline
      }
    }

  val serverPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
        pipeline.addLast("byteEncoder", new ChannelBufferEncoder)
        pipeline.addLast("byteDecoder", new ChannelBufferDecoder)
        pipeline
      }
    }
}
