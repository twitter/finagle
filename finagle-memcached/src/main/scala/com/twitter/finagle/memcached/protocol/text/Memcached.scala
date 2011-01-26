package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.builder.Codec
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._

class Memcached extends Codec[Command, Response] {
  private[this] val storageCommands = collection.Set[ChannelBuffer](
    "set", "add", "replace", "append", "prepend")

  val serverPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("encoder", new server.Encoder)
        pipeline.addLast("decoder", new server.Decoder(storageCommands))
        pipeline
      }
    }
  }


  val clientPipelineFactory = {
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("decoder", new client.Decoder)
        pipeline.addLast("decoding2response", new DecodingToResponse)
        pipeline.addLast("encoder", new client.Encoder)
        pipeline
      }
    }
  }
}