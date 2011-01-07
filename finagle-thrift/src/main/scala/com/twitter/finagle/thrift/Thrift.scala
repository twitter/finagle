package com.twitter.finagle.thrift

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

import com.twitter.finagle.builder.Codec

class ThriftWithWrappedReplies extends Thrift with WrappedReplies

trait WrappedReplies extends Thrift {
  override val wrapReplies = true
}

class Thrift extends Codec
{
  val wrapReplies = false
  val instance = this

  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec",    new ThriftFrameCodec)
        pipeline.addLast("thriftClientEncoder", new ThriftClientEncoder)
        pipeline.addLast("thriftClientDecoder", new ThriftClientDecoder(wrapReplies))
        pipeline
      }
    }

  val serverPipelineFactory =
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
