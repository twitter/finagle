package com.twitter.finagle.http

/**
 * This puts it all together: The HTTP codec itself.
 */

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.http._

import com.twitter.util.StorageUnit
import com.twitter.conversions.storage._

import com.twitter.finagle.Codec

case class Http(
    _compressionLevel: Int = 0,
    _maxChunkAggregationBufferSize: StorageUnit = 1.megabyte)
  extends Codec[HttpRequest, HttpResponse] {

  def compressionLevel(level: Int) = copy(_compressionLevel = level)
  def maxChunkAggregationBufferSize(bufferSize: StorageUnit) =
    copy(_maxChunkAggregationBufferSize = bufferSize)

  val clientPipelineFactory: ChannelPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpClientCodec())
        pipeline.addLast(
          "httpDechunker",
          new HttpChunkAggregator(_maxChunkAggregationBufferSize.inBytes.toInt))

        pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

        pipeline.addLast(
          "connectionLifecycleManager",
          new ClientConnectionManager)

        pipeline
      }
    }

  val serverPipelineFactory =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpServerCodec)
        if (_compressionLevel > 0) {
          pipeline.addLast(
            "httpCompressor",
            new HttpContentCompressor(_compressionLevel))
        }

        // Response to ``Expect: Continue'' requests.
        pipeline.addLast("respondToExpectContinue", new RespondToExpectContinue)
        pipeline.addLast(
          "httpDechunker",
          new HttpChunkAggregator(_maxChunkAggregationBufferSize.inBytes.toInt))

        pipeline.addLast(
          "connectionLifecycleManager",
          new ServerConnectionManager)

        pipeline
      }
    }
}

object Http {
  def apply() = new Http()
}
