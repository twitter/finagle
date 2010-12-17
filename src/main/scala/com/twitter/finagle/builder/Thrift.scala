package com.twitter.finagle.builder

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

import com.twitter.finagle.thrift._

object Thrift extends Codec {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftFrameCodec",    new ThriftFrameCodec)
        pipeline.addLast("thriftClientEncoder", new ThriftClientEncoder)
        pipeline.addLast("thriftClientDecoder", new ThriftClientDecoder)
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
