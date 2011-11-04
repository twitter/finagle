package com.twitter.finagle.stream

import java.util.concurrent.atomic.AtomicBoolean

import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http.{
  HttpServerCodec, HttpClientCodec, HttpRequest, HttpResponse}

import com.twitter.concurrent.Channel
import com.twitter.util.Future

import com.twitter.finagle.{
  Codec, CodecFactory, Service, ServiceProxy}
import com.twitter.finagle.ServiceNotAvailableException

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
          pipeline.addLast("httpChunker", new HttpChunker)
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
          pipeline.addLast("httpDechunker", new HttpDechunker)
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
    private[this] val used = new AtomicBoolean(false)

    override def apply(request: HttpRequest) = {
      if (used.compareAndSet(false, true)) underlying(request) else {
        Future.exception(new ServiceNotAvailableException)
      }
    }

    override def isAvailable = !used.get && underlying.isAvailable
  }
}
