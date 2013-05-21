package com.twitter.finagle.spdy

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.channel.{Channel, ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.jboss.netty.handler.codec.spdy._

import com.twitter.conversions.storage._
import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{ChannelTransport, Transport}
import com.twitter.util.{Closable, StorageUnit}

class AnnotateSpdyStreamId extends SimpleFilter[HttpRequest, HttpResponse] {
  def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
    val streamId = request.getHeader(SpdyHttpHeaders.Names.STREAM_ID)
    service(request) map { response =>
      response.setHeader(SpdyHttpHeaders.Names.STREAM_ID, streamId)
      response
    }
  }
}

class GenerateSpdyStreamId extends SimpleFilter[HttpRequest, HttpResponse] {
  private[this] val currentStreamId = new AtomicInteger(1)

  def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
    SpdyHttpHeaders.setStreamId(request, currentStreamId.getAndAdd(2))
    service(request) map { response =>
      SpdyHttpHeaders.removeStreamId(response)
      response
    }
  }
}

case class Spdy(
    _version: Int = 3,
    _compressionLevel: Int = 6,
    _maxHeaderSize: StorageUnit = 16384.bytes,
    _maxRequestSize: StorageUnit = 5.megabytes,
    _maxResponseSize: StorageUnit = 5.megabytes)
  extends CodecFactory[HttpRequest, HttpResponse]
{
  def version(version: Int) = copy(_version = version)
  def compressionLevel(level: Int) = copy(_compressionLevel = level)
  def maxHeaderSize(size: StorageUnit) = copy(_maxHeaderSize = size)
  def maxRequestSize(size: StorageUnit) = copy(_maxRequestSize = size)
  def maxResponseSize(size: StorageUnit) = copy(_maxResponseSize = size)

  def client = { config =>
    new Codec[HttpRequest, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val maxHeaderSizeInBytes = _maxHeaderSize.inBytes.toInt
          val maxResponseSizeInBytes = _maxResponseSize.inBytes.toInt

          val pipeline = Channels.pipeline()
          pipeline.addLast("spdyFrameCodec",     new SpdyFrameCodec(_version, 8192, maxHeaderSizeInBytes, _compressionLevel, 11, 8))
          pipeline.addLast("spdySessionHandler", new SpdySessionHandler(_version, false))
          pipeline.addLast("spdyHttpCodec",      new SpdyHttpCodec(_version, maxResponseSizeInBytes))
          pipeline
        }
      }

      override def prepareConnFactory(
        underlying: ServiceFactory[HttpRequest, HttpResponse]
      ): ServiceFactory[HttpRequest, HttpResponse] = {
        new GenerateSpdyStreamId andThen super.prepareConnFactory(underlying)
      }

      override def newClientTransport(ch: Channel, statsReceiver: StatsReceiver): Transport[HttpRequest, HttpResponse] =
        new ChannelTransport[HttpRequest, HttpResponse](ch)

      override def newClientDispatcher(transport: Transport[HttpRequest, HttpResponse]) =
        new SpdyClientDispatcher(transport)
    }
  }

  def server = { config =>
    new Codec[HttpRequest, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val maxHeaderSizeInBytes = _maxHeaderSize.inBytes.toInt
          val maxRequestSizeInBytes = _maxRequestSize.inBytes.toInt

          val pipeline = Channels.pipeline()
          pipeline.addLast("spdyFrameCodec",     new SpdyFrameCodec(_version, 8192, maxHeaderSizeInBytes, _compressionLevel, 11, 8))
          pipeline.addLast("spdySessionHandler", new SpdySessionHandler(_version, true))
          pipeline.addLast("spdyHttpCodec",      new SpdyHttpCodec(_version, maxRequestSizeInBytes))
          pipeline
        }
      }

      override def prepareConnFactory(
        underlying: ServiceFactory[HttpRequest, HttpResponse]
      ): ServiceFactory[HttpRequest, HttpResponse] = {
        new AnnotateSpdyStreamId andThen super.prepareConnFactory(underlying)
      }

      override def newServerDispatcher(
          transport: Transport[HttpResponse, HttpRequest],
          service: Service[HttpRequest, HttpResponse]
      ): Closable = new SpdyServerDispatcher(transport, service)
    }
  }
}

object Spdy {
  def get() = Spdy()
}
