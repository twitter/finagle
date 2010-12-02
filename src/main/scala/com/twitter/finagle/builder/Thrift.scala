package com.twitter.finagle.builder

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

import com.twitter.finagle.thrift._

object Thrift extends Codec {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftClientEncoder", new ThriftClientEncoder)
        pipeline.addLast("thriftFramedClientDecoder", new ThriftFramedClientDecoder)
        pipeline
      }
    }

  val serverPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftServerEncoder", new ThriftServerEncoder)
        pipeline.addLast("thriftFramedServerDecoder", new ThriftFramedServerDecoder)
        pipeline
      }
    }
}
