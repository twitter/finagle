package com.twitter.finagle.builder

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.http._

class Http(compressionLevel: Int = 0) extends Codec {
  val clientPipelineFactory: ChannelPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec",        new HttpClientCodec())
        pipeline.addLast("httpDechunker",    new HttpChunkAggregator(10<<20))
        pipeline.addLast("httpDecompressor", new HttpContentDecompressor)
        pipeline
      }
    }

  val serverPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpServerCodec)
        if (compressionLevel > 0) {
          pipeline.addLast(
            "httpCompressor",
            new HttpContentCompressor(compressionLevel))
        }

        pipeline.addLast(
          "connectionLifecycleManager",
          new HttpServerConnectionLifecycleManager)

        pipeline
      }
    }
}

object Http extends Http(0)
object HttpWithCompression extends Http(6)
