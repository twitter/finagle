package com.twitter.finagle.netty4.http

import com.twitter.conversions.storage._
import com.twitter.finagle.http
import com.twitter.finagle.{Status => _, _}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.{AnyToHeapInboundHandlerName, Netty4Listener, Netty4Transporter}
import com.twitter.finagle.netty4.channel.AnyToHeapInboundHandler
import com.twitter.finagle.netty4.http.handler._
import com.twitter.finagle.param.Logger
import com.twitter.finagle.server.Listener
import io.netty.channel._
import io.netty.handler.codec.{http => NettyHttp}
import java.net.SocketAddress

/**
 * The `exp` package contains params to configure the underlying netty version
 * for http 1.1 clients and servers. The netty4 implementation is considered
 * experimental and is untested in production scenarios.
 */
object exp {

  /**
   * The name assigned to a `HttpServerCodec` instance in a netty `ChannelPipeline`
   */
  private[finagle] val HttpCodecName = "httpCodec"

  private[finagle] def initClient(params: Stack.Params): ChannelPipeline => Unit = {
    val maxResponseSize = params[http.param.MaxResponseSize].size
    val decompressionEnabled = params[http.param.Decompression].enabled
    val streaming = params[http.param.Streaming].enabled

    { pipeline: ChannelPipeline =>
      if (decompressionEnabled)
        pipeline.addLast("httpDecompressor", new NettyHttp.HttpContentDecompressor)

      if (streaming) {
        // 8 KB is the size of the maxChunkSize parameter used in netty3,
        // which is where it stops attempting to aggregate messages that lack
        // a 'Transfer-Encoding: chunked' header.
        pipeline.addLast("fixedLenAggregator", new FixedLengthMessageAggregator(8.kilobytes))
      } else {
        pipeline.addLast(
          "httpDechunker",
          new NettyHttp.HttpObjectAggregator(maxResponseSize.inBytes.toInt)
        )
      }
      // Map some client related channel exceptions to something meaningful to finagle
      pipeline.addLast("clientExceptionMapper", ClientExceptionMapper)

      pipeline.addLast(AnyToHeapInboundHandlerName, AnyToHeapInboundHandler)
    }
  }

  private[finagle] val Netty4HttpTransporter: Stack.Params => SocketAddress => Transporter[Any, Any] =
    (params: Stack.Params) =>
      (addr: SocketAddress) => Netty4Transporter.raw(
        pipelineInit = ClientPipelineInit(params),
        addr = addr,
        params = params
      )

  private[finagle] val ClientPipelineInit: Stack.Params => ChannelPipeline => Unit = {
    params: Stack.Params =>
      pipeline: ChannelPipeline => {
        val maxChunkSize = params[http.param.MaxChunkSize].size
        val maxHeaderSize = params[http.param.MaxHeaderSize].size
        val maxInitialLineSize = params[http.param.MaxInitialLineSize].size

        val codec = new NettyHttp.HttpClientCodec(
          maxInitialLineSize.inBytes.toInt,
          maxHeaderSize.inBytes.toInt,
          maxChunkSize.inBytes.toInt
        )

        pipeline.addLast(HttpCodecName, codec)

        initClient(params)(pipeline)
      }
  }

  private[finagle] def initServer(params: Stack.Params): ChannelPipeline => Unit = {
    val maxRequestSize = params[http.param.MaxRequestSize].size
    val decompressionEnabled = params[http.param.Decompression].enabled
    val compressionLevel = params[http.param.CompressionLevel].level
    val streaming = params[http.param.Streaming].enabled
    val log = params[Logger].log

    { pipeline: ChannelPipeline =>

      compressionLevel match {
        case lvl if lvl > 0 =>
          pipeline.addLast("httpCompressor", new NettyHttp.HttpContentCompressor(lvl))
        case -1 =>
          pipeline.addLast("httpCompressor", new TextualContentCompressor)
        case _ =>
      }

      // we decompress before object aggregation so that fixed-length
      // encoded messages aren't re-chunked by the decompressor after
      // aggregation.
      if (decompressionEnabled)
        pipeline.addLast("httpDecompressor", new NettyHttp.HttpContentDecompressor)

      // nb: Netty's http object aggregator handles 'expect: continue' headers
      // and oversize payloads but the base codec does not. Consequently we need to
      // install handlers to replicate this behavior when streaming.
      if (streaming) {
        pipeline.addLast("payloadSizeHandler", new PayloadSizeHandler(maxRequestSize, Some(log)))
        pipeline.addLast("expectContinue", RespondToExpectContinue)
        pipeline.addLast("fixedLenAggregator", new FixedLengthMessageAggregator(maxRequestSize))
      }
      else
        pipeline.addLast(
          "httpDechunker",
          new NettyHttp.HttpObjectAggregator(maxRequestSize.inBytes.toInt)
        )

      // We need to handle bad requests as the dispatcher doesn't know how to handle them.
      pipeline.addLast("badRequestHandler", BadRequestHandler)

      pipeline.addLast(AnyToHeapInboundHandlerName, AnyToHeapInboundHandler)
    }
  }

  private[finagle] val ServerPipelineInit: Stack.Params => ChannelPipeline => Unit = {
    params: Stack.Params =>
      pipeline: ChannelPipeline => {
        val maxInitialLineSize = params[http.param.MaxInitialLineSize].size
        val maxHeaderSize = params[http.param.MaxHeaderSize].size
        val maxRequestSize = params[http.param.MaxRequestSize].size

        val codec = new NettyHttp.HttpServerCodec(
          maxInitialLineSize.inBytes.toInt,
          maxHeaderSize.inBytes.toInt,
          maxRequestSize.inBytes.toInt
        )

        pipeline.addLast(HttpCodecName, codec)

        initServer(params)(pipeline)
      }
  }

  private[finagle] val Netty4HttpListener: Stack.Params => Listener[Any, Any] = (params: Stack.Params) =>
    Netty4Listener[Any, Any](
      params = params,
      pipelineInit = ServerPipelineInit(params)
    )
}
