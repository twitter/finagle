package com.twitter.finagle.netty4

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.http.handler.ClientExceptionMapper
import com.twitter.finagle.netty4.http.handler.FixedLengthMessageAggregator
import com.twitter.finagle.netty4.http.handler.HeaderValidatorHandler
import com.twitter.finagle.netty4.http.handler.UnpoolHttpHandler
import com.twitter.finagle.netty4.http.handler.UriValidatorHandler
import com.twitter.finagle.http.param._
import com.twitter.finagle.netty4.http.handler.BadRequestHandler
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.TransportContext
import io.netty.channel._
import io.netty.handler.codec.http._
import java.net.SocketAddress

/**
 * Finagle HTTP implementation based on Netty 4.
 */
package object http {

  /**
   * The name assigned to a `HttpServerCodec` instance in a netty `ChannelPipeline`
   */
  private[finagle] val HttpCodecName = "httpCodec"

  /**
   * The name assigned to an `Http2MultiplexCodec` instance in a netty `ChannelPipeline`
   */
  private[finagle] val Http2CodecName = "http2Codec"
  private[finagle] val Http2MultiplexHandlerName = "Http2MultiplexHandler"

  private[finagle] def newHttpClientCodec(params: Stack.Params): HttpClientCodec = {
    val maxInitialLineSize = params[MaxInitialLineSize].size
    val maxHeaderSize = params[MaxHeaderSize].size

    // We unset the limit for maxChunkSize (8k by default) so Netty emits entire available
    // payload as a single chunk instead of splitting it. This way we put the data into use
    // quicker, the moment it's available.
    new HttpClientCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      Int.MaxValue, /* maxChunkSize */
      false, /* failOnMissingResponse */
      false /* validateHeaders, we validate in HeaderValidatorHandler */
    )
  }

  private[finagle] def newHttpServerCodec(params: Stack.Params): HttpServerCodec = {
    val maxInitialLineSize = params[MaxInitialLineSize].size
    val maxHeaderSize = params[MaxHeaderSize].size

    // We unset the limit for maxChunkSize (8k by default) so Netty emits entire available
    // payload as a single chunk instead of splitting it. This way we put the data into use
    // quicker, the moment it's available.
    new HttpServerCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      Int.MaxValue, /* maxChunkSize */
      false /* validateHeaders, we validate in HeaderValidatorHandler */
    )
  }

  /**
   * Initialize the client pipeline, adding handlers _before_ the specified role.
   *
   * @see `initClientFn` for details about what is added.
   */
  private[finagle] def initClientBefore(
    role: String,
    params: Stack.Params
  ): ChannelPipeline => Unit = { pipeline => initClientFn(params, pipeline.addBefore(role, _, _)) }

  /** Initialize the client pipeline by adding elements to the end of the pipeline
   *
   * @see `initClientFn` for details about what is added.
   */
  private[finagle] def initClient(params: Stack.Params): ChannelPipeline => Unit = { pipeline =>
    initClientFn(params, pipeline.addLast(_, _))
  }

  /** Initialize the client pipeline accessories including decompression, dechunking, etc. */
  private[finagle] def initClientFn(
    params: Stack.Params,
    fn: (String, ChannelHandler) => Unit
  ): Unit = {
    val maxResponseSize = params[MaxResponseSize].size
    val decompressionEnabled = params[Decompression].enabled

    if (decompressionEnabled) {
      fn("httpDecompressor", new HttpContentDecompressor)
    }

    params[Streaming] match {
      case Streaming.Enabled(fixedLengthStreamedAfter) =>
        fn(
          "fixedLenAggregator",
          new FixedLengthMessageAggregator(fixedLengthStreamedAfter)
        )
      case Streaming.Disabled =>
        fn(
          "httpDechunker",
          new HttpObjectAggregator(maxResponseSize.inBytes.toInt)
        )
    }

    // We're going to validate our headers right before the client exception mapper.
    fn(HeaderValidatorHandler.HandlerName, HeaderValidatorHandler)

    // Let's see if we can filter out bad URI's before Netty starts handling them...
    fn(UriValidatorHandler.HandlerName, UriValidatorHandler)

    // Map some client related channel exceptions to something meaningful to finagle
    fn("clientExceptionMapper", ClientExceptionMapper)

    // Given that Finagle's channel transports aren't doing anything special (yet)
    // about resource management, we have to turn pooled resources into unpooled ones as
    // the very last step of the pipeline.
    fn("unpoolHttp", UnpoolHttpHandler)
  }

  private[finagle] val ClientPipelineInit: Stack.Params => ChannelPipeline => Unit = {
    params: Stack.Params => pipeline: ChannelPipeline =>
      {
        pipeline.addLast(HttpCodecName, newHttpClientCodec(params))
        initClient(params)(pipeline)
      }
  }

  private[finagle] val Netty4HttpTransporter: Stack.Params => SocketAddress => Transporter[
    Any,
    Any,
    TransportContext
  ] =
    (params: Stack.Params) =>
      (addr: SocketAddress) =>
        Netty4Transporter.raw(
          pipelineInit = ClientPipelineInit(params),
          addr = addr,
          params = params
        )

  private[finagle] def initServer(params: Stack.Params): ChannelPipeline => Unit = {
    val autoContinue = params[AutomaticContinue].enabled
    val maxRequestSize = params[MaxRequestSize].size
    val decompressionEnabled = params[Decompression].enabled
    val compressionLevel = params[CompressionLevel].level
    val stats = params[Stats].statsReceiver
    val badRequestHandler = new BadRequestHandler(stats)

    { pipeline: ChannelPipeline =>
      compressionLevel match {
        case lvl if lvl > 0 =>
          pipeline.addLast("httpCompressor", new HttpContentCompressor(lvl))
        case -1 =>
          pipeline.addLast("httpCompressor", new TextualContentCompressor)
        case _ =>
      }

      // we decompress before object aggregation so that fixed-length
      // encoded messages aren't re-chunked by the decompressor after
      // aggregation.
      if (decompressionEnabled)
        pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

      // nb: Netty's http object aggregator handles 'expect: continue' headers
      // and oversize payloads but the base codec does not. Consequently we need to
      // install handlers to replicate this behavior when streaming.
      params[Streaming] match {
        case Streaming.Enabled(fixedLengthStreamedAfter) =>
          if (autoContinue)
            pipeline.addLast("expectContinue", new HttpServerExpectContinueHandler)

          // no need to handle expect headers in the fixedLenAggregator since we have the task
          // specific HttpServerExpectContinueHandler above.
          pipeline.addLast(
            "fixedLenAggregator",
            new FixedLengthMessageAggregator(fixedLengthStreamedAfter, handleExpectContinue = false)
          )
        case Streaming.Disabled =>
          pipeline.addLast(
            "httpDechunker",
            new FinagleHttpObjectAggregator(
              maxRequestSize.inBytes.toInt,
              handleExpectContinue = autoContinue
            )
          )
      }

      // Let's see if we can filter out bad URI's before Netty starts handling them...
      pipeline.addLast(UriValidatorHandler.HandlerName, UriValidatorHandler)

      // We're going to validate our headers right before the bad request handler.
      pipeline.addLast(HeaderValidatorHandler.HandlerName, HeaderValidatorHandler)

      // We need to handle bad requests as the dispatcher doesn't know how to handle them.
      pipeline.addLast(BadRequestHandler.HandlerName, badRequestHandler)

      // Given that Finagle's channel transports aren't doing anything special (yet)
      // about resource management, we have to turn pooled resources into unpooled ones as
      // the very last step of the pipeline.
      pipeline.addLast("unpoolHttp", UnpoolHttpHandler)
    }
  }

  private[finagle] val ServerPipelineInit: Stack.Params => ChannelPipeline => Unit = {
    params: Stack.Params => pipeline: ChannelPipeline =>
      {
        pipeline.addLast(HttpCodecName, newHttpServerCodec(params))
        initServer(params)(pipeline)
      }
  }

  private[finagle] val Netty4HttpListener: Stack.Params => Listener[Any, Any, TransportContext] =
    (params: Stack.Params) =>
      Netty4Listener[Any, Any](
        pipelineInit = ServerPipelineInit(params),
        params = params
      )
}
