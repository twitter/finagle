package com.twitter.finagle.http

/**
 * This puts it all together: The HTTP codec itself.
 */

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.http._

import com.twitter.util.StorageUnit
import com.twitter.conversions.storage._

import com.twitter.finagle.{Codec, ClientCodec, ServerCodec}

case class Http(
    _compressionLevel: Int = 0,
    _maxRequestSize: StorageUnit = 1.megabyte,
    _maxResponseSize: StorageUnit = 1.megabyte,
    _decompressionEnabled: Boolean = true)
  extends Codec[HttpRequest, HttpResponse] {

  def compressionLevel(level: Int) = copy(_compressionLevel = level)
  def maxRequestSize(bufferSize: StorageUnit) = copy(_maxRequestSize = bufferSize)
  def maxResponseSize(bufferSize: StorageUnit) = copy(_maxResponseSize = bufferSize)
  def decompressionEnabled(yesno: Boolean) = copy(_decompressionEnabled = yesno)

  override def clientCodec = new ClientCodec[HttpRequest, HttpResponse] {
    def pipelineFactory = new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("httpCodec", new HttpClientCodec())
        pipeline.addLast(
          "httpDechunker",
          new HttpChunkAggregator(_maxResponseSize.inBytes.toInt))

        if (_decompressionEnabled)
          pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

        pipeline.addLast(
          "connectionLifecycleManager",
          new ClientConnectionManager)

        pipeline
      }
    }
  }

  override def serverCodec = new ServerCodec[HttpRequest, HttpResponse] {
    def pipelineFactory = new ChannelPipelineFactory {
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
          new HttpChunkAggregator(_maxRequestSize.inBytes.toInt))

        pipeline.addLast(
          "connectionLifecycleManager",
          new ServerConnectionManager)

        pipeline
      }
    }
  }
}

object Http {
  def apply() = new Http()
}
