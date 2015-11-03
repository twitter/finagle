package com.twitter.finagle.spdy

import com.twitter.conversions.storage._
import com.twitter.finagle._
import com.twitter.finagle.netty3.transport.ChannelTransport
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, StorageUnit}
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.channel.{Channel, ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.jboss.netty.handler.codec.spdy._

class AnnotateSpdyStreamId extends SimpleFilter[HttpRequest, HttpResponse] {
  def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
    val streamId = request.headers.get(SpdyHttpHeaders.Names.STREAM_ID)
    service(request) map { response =>
      response.headers.set(SpdyHttpHeaders.Names.STREAM_ID, streamId)
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
    _version: SpdyVersion = SpdyVersion.SPDY_3_1,
    _enableHeaderCompression: Boolean = true,
    _maxHeaderSize: StorageUnit = 16384.bytes,
    _maxRequestSize: StorageUnit = 5.megabytes,
    _maxResponseSize: StorageUnit = 5.megabytes)
  extends CodecFactory[HttpRequest, HttpResponse]
{
  def version(version: SpdyVersion) = copy(_version = version)
  def enableHeaderCompression(enable: Boolean) = copy(_enableHeaderCompression = enable)
  def maxHeaderSize(size: StorageUnit) = copy(_maxHeaderSize = size)
  def maxRequestSize(size: StorageUnit) = copy(_maxRequestSize = size)
  def maxResponseSize(size: StorageUnit) = copy(_maxResponseSize = size)

  private[this] def spdyFrameCodec = {
    val maxHeaderSizeInBytes = _maxHeaderSize.inBytes.toInt
    if (_enableHeaderCompression) {
      // Header blocks tend to be small so reduce the window-size of the
      // compressor from 32 KB (15) to 2KB (11) to save memory.
      // These settings still provide sufficient compression to fit the
      // compressed header block within the TCP initial congestion window.
      new SpdyFrameCodec(_version, 8192, maxHeaderSizeInBytes, 9, 11, 8)
    } else {
      new SpdyRawFrameCodec(_version, 8192, maxHeaderSizeInBytes)
    }
  }

  def client = { config =>
    new Codec[HttpRequest, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val maxHeaderSizeInBytes = _maxHeaderSize.inBytes.toInt
          val maxResponseSizeInBytes = _maxResponseSize.inBytes.toInt

          val pipeline = Channels.pipeline()
          pipeline.addLast("spdyFrameCodec",     spdyFrameCodec)
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

      override def newClientTransport(ch: Channel, statsReceiver: StatsReceiver): Transport[Any, Any] =
        new ChannelTransport(ch)

      override def newClientDispatcher(transport: Transport[Any, Any]) =
        new SpdyClientDispatcher(Transport.cast[HttpRequest, HttpResponse](transport))
    }
  }

  def server = { config =>
    new Codec[HttpRequest, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val maxRequestSizeInBytes = _maxRequestSize.inBytes.toInt

          val pipeline = Channels.pipeline()
          pipeline.addLast("spdyFrameCodec",     spdyFrameCodec)
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
          transport: Transport[Any, Any],
          service: Service[HttpRequest, HttpResponse]
      ): Closable = new SpdyServerDispatcher(
        Transport.cast[HttpResponse, HttpRequest](transport), service)
    }
  }

  override val protocolLibraryName: String = "spdy"
}

object Spdy {
  def get() = Spdy()
}
