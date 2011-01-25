package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol.text.{server, client}

class Kestrel extends Codec[Command, Response] {
  private[this] val responseVocabulary = new KestrelResponseVocabulary
  private[this] val commandVocabulary = new KestrelCommandVocabulary

  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("encoder", new server.Encoder)
        pipeline.addLast("decoder", new server.Decoder(commandVocabulary))
        pipeline
      }
    }
  }


  val clientPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new client.Decoder(responseVocabulary))
        pipeline.addLast("encoder", new client.Encoder)
        pipeline
      }
    }
  }
}