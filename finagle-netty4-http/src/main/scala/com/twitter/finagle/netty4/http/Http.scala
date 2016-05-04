package com.twitter.finagle.netty4.http

import com.twitter.finagle.Http.param.HttpImpl
import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.{Status => _, _}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.{Netty4Listener, Netty4Transporter}
import com.twitter.finagle.server.Listener
import io.netty.channel._
import io.netty.handler.codec.{http => NettyHttp}

/**
 * The `exp` package contains params to configure the underlying netty version
 * for http 1.1 clients and servers. The netty4 implementation is considered
 * experimental and is untested in production scenarios.
 */
object exp {

  private[http] val Netty4HttpTransporter: Stack.Params => Transporter[Any, Any] =

    (params: Stack.Params) => {
      val maxChunkSize = params[httpparam.MaxChunkSize].size
      val maxHeaderSize = params[httpparam.MaxHeaderSize].size
      val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size
      val maxResponseSize = params[httpparam.MaxResponseSize].size
      val decompressionEnabled = params[httpparam.Decompression].enabled
      val streaming = params[httpparam.Streaming].enabled

      val pipelineCb: ChannelPipeline => Unit = { pipeline: ChannelPipeline =>
        val codec = new NettyHttp.HttpClientCodec(
          maxInitialLineSize.inBytes.toInt,
          maxHeaderSize.inBytes.toInt,
          maxChunkSize.inBytes.toInt
        )

        pipeline.addLast("httpCodec", codec)

        if (!streaming)
          pipeline.addLast(
            "httpDechunker",
            new NettyHttp.HttpObjectAggregator(maxResponseSize.inBytes.toInt)
          )

        if (decompressionEnabled)
          pipeline.addLast("httpDecompressor", new NettyHttp.HttpContentDecompressor)
      }

      Netty4Transporter(pipelineCb, params)
    }

  private[finagle] val Netty4HttpListener: Stack.Params => Listener[Any, Any] = (params: Stack.Params) => {
      val maxChunkSize = params[httpparam.MaxChunkSize].size
      val maxHeaderSize = params[httpparam.MaxHeaderSize].size
      val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size
      val maxRequestSize = params[httpparam.MaxRequestSize].size
      val decompressionEnabled = params[httpparam.Decompression].enabled
      val compressionLevel = params[httpparam.CompressionLevel].level
      val streaming = params[httpparam.Streaming].enabled

      val init: ChannelPipeline => Unit = { pipeline: ChannelPipeline =>
        // todo: channel buffer manager handler CSL-2722
        val codec = new NettyHttp.HttpServerCodec(
          maxInitialLineSize.inBytes.toInt,
          maxHeaderSize.inBytes.toInt,
          maxChunkSize.inBytes.toInt
        )

        pipeline.addLast("httpCodec", codec)

        compressionLevel match {
          case lvl if lvl > 0 =>
            pipeline.addLast("httpCompressor", new NettyHttp.HttpContentCompressor(lvl))
          case lvl if lvl == -1 =>
            pipeline.addLast("httpCompressor", new TextualContentCompressor)
          case _ =>
        }

        if (decompressionEnabled)
          pipeline.addLast("httpDecompressor", new NettyHttp.HttpContentDecompressor)


        // nb: Netty's http object aggregator handles 'expect: continue' headers
        // but its request chunk decoder doesn't. Consequently we need to install
        // this handler when we're not aggregating content chunks.
        if (streaming)
          pipeline.addLast("expectContinue", RespondToExpectContinue)
        else
          pipeline.addLast(
            "httpDechunker",
            new NettyHttp.HttpObjectAggregator(maxRequestSize.inBytes.toInt)
          )
      }

      Netty4Listener[Any, Any](
        params = params,
        pipelineInit = init
      )
    }

  val Netty4Impl: HttpImpl =
    HttpImpl(
      new Netty4ClientStreamTransport(_),
      new Netty4ServerStreamTransport(_),
      Netty4HttpTransporter,
      Netty4HttpListener
    )
}
