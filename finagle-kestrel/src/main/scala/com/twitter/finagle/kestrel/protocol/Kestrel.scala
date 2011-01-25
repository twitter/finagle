package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol.text.{MemcachedCommandVocabulary, server, client}
import com.twitter.finagle.memcached.protocol.MemcachedResponseVocabulary

class Kestrel extends Codec[Command, Response] {
  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("encoder", new server.Encoder)
        pipeline.addLast("decoder", new server.Decoder(new MemcachedCommandVocabulary))
        pipeline
      }
    }
  }


  val clientPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new client.Decoder(new MemcachedResponseVocabulary))
        pipeline.addLast("encoder", new client.Encoder)
        pipeline
      }
    }
  }
}