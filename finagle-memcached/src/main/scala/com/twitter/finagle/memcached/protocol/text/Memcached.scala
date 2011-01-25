package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol._

class Memcached extends Codec[Command, Response] {
  private[this] val responseParser = new MemcachedResponseVocabulary
  private[this] val commandParser = new MemcachedCommandVocabulary

  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("encoder", new server.Encoder)
        pipeline.addLast("decoder", new server.Decoder(commandParser))
        pipeline
      }
    }
  }


  val clientPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new client.Decoder(responseParser))
        pipeline.addLast("encoder", new client.Encoder)
        pipeline
      }
    }
  }
}