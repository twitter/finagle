package com.twitter.finagle.stream

import com.twitter.finagle.{Codec, ClientCodec, ServerCodec}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http.{HttpServerCodec, HttpClientCodec, HttpRequest}
import org.jboss.netty.buffer.ChannelBuffer

object Stream extends Stream

class Stream extends Codec[HttpRequest, com.twitter.concurrent.Channel[ChannelBuffer]] {
  override def serverCodec =
    new ServerCodec[HttpRequest, com.twitter.concurrent.Channel[ChannelBuffer]] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpServerCodec)
          pipeline.addLast("dechannelize", new ChannelToHttpChunk)
          pipeline
        }
      }
    }

  override def clientCodec =
    new ClientCodec[HttpRequest, com.twitter.concurrent.Channel[ChannelBuffer]] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpClientCodec)
          pipeline.addLast("bechannelfy", new HttpChunkToChannel)
          pipeline
        }
      }
    }
}
