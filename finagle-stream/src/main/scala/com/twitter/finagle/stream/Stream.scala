package com.twitter.finagle.stream

import com.twitter.concurrent.Channel
import com.twitter.finagle.{Codec, ClientCodec, ServerCodec, Service, ServiceProxy}
import com.twitter.util.Future
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
      override def prepareService(
        underlying: Service[HttpRequest, StreamResponse]
      ): Future[Service[HttpRequest, StreamResponse]] = {
        Future.value(new UseOnceService(underlying))
      }
    }

  private class UseOnceService(underlying: Service[HttpRequest, StreamResponse])
    extends ServiceProxy[HttpRequest, StreamResponse](underlying)
  {
    private var used = false

    override def apply(request: HttpRequest) = {
      require(used == false)
      used = true
      underlying(request)
    }

    override def isAvailable = {
      !used && underlying.isAvailable
    }
  }
}
