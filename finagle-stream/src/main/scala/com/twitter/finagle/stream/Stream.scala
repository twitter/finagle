package com.twitter.finagle.stream

import com.twitter.concurrent.Channel
import com.twitter.finagle.{
  Codec, CodecFactory, Service, ServiceProxy}
import com.twitter.util.Future
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http.{HttpServerCodec, HttpClientCodec, HttpRequest, HttpResponse}

object Stream {
  def apply(): Stream = new Stream()
  def get() = apply()
}

class Stream extends CodecFactory[HttpRequest, StreamResponse] {
  def server = Function.const {
    new Codec[HttpRequest, StreamResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpServerCodec)
          pipeline.addLast("dechannelize", new ChannelToHttpChunk)
          pipeline
        }
      }
    }
  }

 def client = Function.const {
    new Codec[HttpRequest, StreamResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpClientCodec)
          pipeline.addLast("bechannelfy", new HttpChunkToChannel)
          pipeline
        }
      }
      override def prepareService(
        underlying: Service[HttpRequest, StreamResponse]
      ): Future[Service[HttpRequest, StreamResponse]] = {
        Future.value(new UseOnceService(underlying))
      }
    }
  }

  private class UseOnceService(underlying: Service[HttpRequest, StreamResponse])
    extends ServiceProxy[HttpRequest, StreamResponse](underlying)
  {
    @volatile private[this] var used = false

    override def apply(request: HttpRequest) = {
      synchronized {
        require(used == false)
        used = true
      }
      underlying(request)
    }

    override def isAvailable = {
      !synchronized(used) && underlying.isAvailable
    }
  }
}
