package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol.text.{server, client}
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._

class Kestrel extends Codec[Command, Response] {
  private[this] val storageCommands = collection.Set[ChannelBuffer]("set")

  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("encoder", new server.Encoder)
        pipeline.addLast("decoder", new server.Decoder(storageCommands))
        pipeline.addLast("decoding2command", new DecodingToCommand)
        pipeline
      }
    }
  }


  val clientPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new client.Decoder)
        pipeline.addLast("decoding2command", new DecodingToResponse)
        pipeline.addLast("encoder", new client.Encoder)
        pipeline
      }
    }
  }
}