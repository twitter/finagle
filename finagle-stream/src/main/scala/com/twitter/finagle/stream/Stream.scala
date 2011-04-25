package com.twitter.finagle.stream

import com.twitter.concurrent.Channel
import com.twitter.finagle.{Codec, ClientCodec, ServerCodec}
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http.{HttpServerCodec, HttpClientCodec, HttpRequest, HttpResponse}

object Stream extends Stream

class Stream extends Codec[HttpRequest, StreamResponse] {
  override def serverCodec =
    new ServerCodec[HttpRequest, StreamResponse] {
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
    new ClientCodec[HttpRequest, StreamResponse] {
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
