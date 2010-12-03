package com.twitter.finagle.builder

import javax.net.ssl.SSLEngine

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.ssl.SslHandler

import com.twitter.finagle.http.{RequestLifecycleSpy, Ssl}

class Http extends Codec {
  val clientPipelineFactory: ChannelPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpClientCodec())
        pipeline.addLast("httpDechunker", new HttpChunkAggregator(10<<20))
        pipeline.addLast("lifecycleSpy", RequestLifecycleSpy)
        pipeline
      }
    }

  val sslServerContext = Ssl.newServerContext()

  val serverPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val compressionLevel = 3 // 0-9, 6 being the default in Netty
        val engine = sslServerContext.createSSLEngine()
        engine.setUseClientMode(false)

        val pipeline = Channels.pipeline()
        pipeline.addLast("ssl", new SslHandler(engine))
        pipeline.addLast("httpCodec", new HttpServerCodec)
        // pipeline.addLast("compressor", new HttpContentCompressor(compressionLevel))
        pipeline.addLast("lifecycleSpy", RequestLifecycleSpy)
        pipeline
      }
    }
}

object Http extends Http
