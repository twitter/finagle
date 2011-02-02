package com.twitter.finagle.stream

import com.twitter.finagle.Codec
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http.{HttpServerCodec, HttpClientCodec, HttpRequest}
import org.jboss.netty.buffer.ChannelBuffer

class Stream extends Codec[HttpRequest, com.twitter.concurrent.Channel[ChannelBuffer]] {
  val serverPipelineFactory = new ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("httpCodec", new HttpServerCodec)
      pipeline.addLast("dechannelize", new ChannelToHttpChunk)
      pipeline
    }
  }

  val clientPipelineFactory = new ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("httpCodec", new HttpClientCodec)
      pipeline.addLast("bechannelfy", new HttpChunkToChannel)
      pipeline
    }
  }
}