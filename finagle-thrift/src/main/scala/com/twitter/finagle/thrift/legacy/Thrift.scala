package com.twitter.finagle.thrift

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

import com.twitter.finagle.Codec

class Thrift extends Codec[ThriftCall[_, _], ThriftReply[_]]
{
  val wrapReplies = false
  val instance = this

  override val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec",    new ThriftFrameCodec)
        pipeline.addLast("thriftClientEncoder", new ThriftClientEncoder)
        pipeline.addLast("thriftClientDecoder", new ThriftClientDecoder)
        pipeline
      }
    }

  override val serverPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec",    new ThriftFrameCodec)
        pipeline.addLast("thriftServerEncoder", new ThriftServerEncoder)
        pipeline.addLast("thriftServerDecoder", new ThriftServerDecoder)
        pipeline
      }
    }
}
