package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel._

class Kestrel extends Codec[Command, Response] {
  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("encoder", new server.Encoder)
        pipeline.addLast("decoder", new server.Decoder)
        pipeline
      }
    }
  }


  val clientPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new client.Decoder)
        pipeline.addLast("encoder", new client.Encoder)
        pipeline
      }
    }
  }
}