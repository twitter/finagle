package com.twitter.finagle.builder

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.http.RequestLifecycleSpy

class Http(val compressionLevel: Int = 0) extends Codec {
  val clientPipelineFactory: ChannelPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpClientCodec())
        pipeline.addLast("httpDechunker",  new HttpChunkAggregator(10<<20))
        pipeline.addLast("httpDecompressor", new HttpContentDecompressor)
        pipeline.addLast("lifecycleSpy", RequestLifecycleSpy)
        pipeline
      }
    }

  val serverPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpServerCodec)
        if (compressionLevel > 0)
          pipeline.addLast("httpCompressor",
                           new HttpContentCompressor(compressionLevel))
        pipeline.addLast("lifecycleSpy", RequestLifecycleSpy)
        pipeline
      }
    }
}

object Http extends Http

object HttpWithCompression extends Http(6)
