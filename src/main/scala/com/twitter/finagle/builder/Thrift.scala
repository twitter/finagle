package com.twitter.finagle.builder

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

import com.twitter.finagle.thrift._

object Thrift extends Codec {
  val clientPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftCodec", new ThriftClientCodec)
        pipeline
      }
    }

  val serverPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("thriftCodec", new ThriftServerCodec)
        pipeline
      }
    }
}
