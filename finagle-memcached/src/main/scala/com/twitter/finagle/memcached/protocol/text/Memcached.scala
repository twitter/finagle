package com.twitter.finagle.memcached.protocol.text

import client.DecodingToResponse
import com.twitter.finagle.{CodecFactory, Codec}
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import server.DecodingToCommand
import server.{Decoder => ServerDecoder}
import client.{Decoder => ClientDecoder}

object Memcached {
  def apply() = new Memcached
  def get() = apply()
}

class Memcached extends CodecFactory[Command, Response] {
  private[this] val storageCommands = collection.Set[ChannelBuffer](
    "set", "add", "replace", "append", "prepend")

  def server = Function.const {
    new Codec[Command, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()

  //        pipeline.addLast("exceptionHandler", new ExceptionHandler)

          pipeline.addLast("decoder", new ServerDecoder(storageCommands))
          pipeline.addLast("decoding2command", new DecodingToCommand)

          pipeline.addLast("encoder", new Encoder)
          pipeline.addLast("response2encoding", new ResponseToEncoding)
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[Command, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()

          pipeline.addLast("decoder", new ClientDecoder)
          pipeline.addLast("decoding2response", new DecodingToResponse)

          pipeline.addLast("encoder", new Encoder)
          pipeline.addLast("command2encoding", new CommandToEncoding)
          pipeline
        }
      }
    }
  }
}

