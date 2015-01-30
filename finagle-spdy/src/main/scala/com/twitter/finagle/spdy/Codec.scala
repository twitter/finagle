package com.twitter.finagle.spdy

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.channel.{Channel, ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http.{HttpRequest => HttpAsk, HttpResponse}
import org.jboss.netty.handler.codec.spdy._

import com.twitter.conversions.storage._
import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{ChannelTransport, Transport}
import com.twitter.util.{Closable, StorageUnit}

class AnnotateSpdyStreamId extends SimpleFilter[HttpAsk, HttpResponse] {
  def apply(request: HttpAsk, service: Service[HttpAsk, HttpResponse]) = {
    val streamId = request.headers.get(SpdyHttpHeaders.Names.STREAM_ID)
    service(request) map { response =>
      response.headers.set(SpdyHttpHeaders.Names.STREAM_ID, streamId)
      response
    }
  }
}

class GenerateSpdyStreamId extends SimpleFilter[HttpAsk, HttpResponse] {
  private[this] val currentStreamId = new AtomicInteger(1)

  def apply(request: HttpAsk, service: Service[HttpAsk, HttpResponse]) = {
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
    _maxAskSize: StorageUnit = 5.megabytes,
    _maxResponseSize: StorageUnit = 5.megabytes)
  extends CodecFactory[HttpAsk, HttpResponse]
{
  def version(version: SpdyVersion) = copy(_version = version)
  def enableHeaderCompression(enable: Boolean) = copy(_enableHeaderCompression = enable)
  def maxHeaderSize(size: StorageUnit) = copy(_maxHeaderSize = size)
  def maxAskSize(size: StorageUnit) = copy(_maxAskSize = size)
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
    new Codec[HttpAsk, HttpResponse] {
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
        underlying: ServiceFactory[HttpAsk, HttpResponse]
      ): ServiceFactory[HttpAsk, HttpResponse] = {
        new GenerateSpdyStreamId andThen super.prepareConnFactory(underlying)
      }

      override def newClientTransport(ch: Channel, statsReceiver: StatsReceiver): Transport[Any, Any] =
        new ChannelTransport(ch)

      override def newClientDispatcher(transport: Transport[Any, Any]) =
        new SpdyClientDispatcher(transport.cast[HttpAsk, HttpResponse])
    }
  }

  def server = { config =>
    new Codec[HttpAsk, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val maxAskSizeInBytes = _maxAskSize.inBytes.toInt

          val pipeline = Channels.pipeline()
          pipeline.addLast("spdyFrameCodec",     spdyFrameCodec)
          pipeline.addLast("spdySessionHandler", new SpdySessionHandler(_version, true))
          pipeline.addLast("spdyHttpCodec",      new SpdyHttpCodec(_version, maxAskSizeInBytes))
          pipeline
        }
      }

      override def prepareConnFactory(
        underlying: ServiceFactory[HttpAsk, HttpResponse]
      ): ServiceFactory[HttpAsk, HttpResponse] = {
        new AnnotateSpdyStreamId andThen super.prepareConnFactory(underlying)
      }

      override def newServerDispatcher(
          transport: Transport[Any, Any],
          service: Service[HttpAsk, HttpResponse]
      ): Closable = new SpdyServerDispatcher(
        transport.cast[HttpResponse, HttpAsk], service)
    }
  }
}

object Spdy {
  def get() = Spdy()
}
