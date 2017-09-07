package com.twitter.finagle.netty3.http

import com.twitter.conversions.storage._
import com.twitter.finagle
import com.twitter.finagle._
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.http.codec._
import com.twitter.finagle.http.filter.{
  ClientContextFilter,
  DtabFilter,
  HttpNackFilter,
  ServerContextFilter
}
import com.twitter.finagle.http.{
  DelayedReleaseService,
  HttpClientTraceInitializer,
  HttpServerTraceInitializer,
  HttpTransport,
  Request,
  Response
}
import com.twitter.finagle.stats.{NullStatsReceiver, ServerStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, StorageUnit}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.codec.http._

/**
 * a HttpChunkAggregator which recovers decode failures into 4xx http responses
 */
private[http] class SafeServerHttpChunkAggregator(maxContentSizeBytes: Int)
    extends HttpChunkAggregator(maxContentSizeBytes) {

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent): Unit = {
    try super.handleUpstream(ctx, e)
    catch {
      case ex: TooLongFrameException =>
        val event = BadMessageConverter.errorToDownstreamEvent(ctx.getChannel, ex)
        ctx.sendDownstream(event)
    }
  }
}

/** Convert exceptions to BadHttpRequests */
private[http] class SafeHttpServerCodec(
  maxInitialLineLength: Int,
  maxHeaderSize: Int,
  maxChunkSize: Int
) extends HttpServerCodec(maxInitialLineLength, maxHeaderSize, maxChunkSize) {
  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    // this only catches Codec exceptions -- when a handler calls sendUpStream(), it
    // rescues exceptions from the upstream handlers and calls notifyHandlerException(),
    // which doesn't throw exceptions.
    try super.handleUpstream(ctx, e)
    catch {
      case ex: Exception =>
        val event = BadMessageConverter.errorToDownstreamEvent(ctx.getChannel, ex)

        // The event must be handled by `this` because we are the codec
        // responsible for handling http messages
        this.handleDownstream(ctx, event)
    }
  }
}

/**
 * @param _compressionLevel The compression level to use. If passed the default value (-1) then use
 * [[com.twitter.finagle.http.codec.TextualContentCompressor TextualContentCompressor]] which will
 * compress text-like content-types with the default compression level (6). Otherwise, use
 * [[org.jboss.netty.handler.codec.http.HttpContentCompressor HttpContentCompressor]] for all
 * content-types with specified compression level.
 *
 * @param _maxRequestSize The maximum size of the inbound request an HTTP server constructed with
 * this codec can receive (default is 5 megabytes). Should be less than 2 gigabytes (up to
 * `Int.MaxValue` bytes). Use streaming/chunked requests to handle larger messages.
 *
 * @param _maxResponseSize The maximum size of the inbound response an HTTP client constructed with
 * this codec can receive (default is 5 megabytes). Should be less than 2 gigabytes (up to
 * `Int.MaxValue` bytes). Use streaming/chunked requests to handle larger messages.
 *
 * @param _streaming Streaming allows applications to work with HTTP messages
 * that have large (or infinite) content bodies. When this flag is set to
 * `true`, the message content is available through a [[com.twitter.io.Reader]],
 * which gives the application a handle to the byte stream. If `false`, the
 * entire message content is buffered into a [[com.twitter.io.Buf]].
 */
private case class HttpCodecFactory(
  _compressionLevel: Int = -1,
  _maxRequestSize: StorageUnit = 5.megabytes,
  _maxResponseSize: StorageUnit = 5.megabytes,
  _decompressionEnabled: Boolean = true,
  _channelBufferUsageTracker: Option[ChannelBufferUsageTracker] = None,
  _enableTracing: Boolean = false,
  _maxInitialLineLength: StorageUnit = 4096.bytes,
  _maxHeaderSize: StorageUnit = 8192.bytes,
  _streaming: Boolean = false,
  _statsReceiver: StatsReceiver = NullStatsReceiver
) extends CodecFactory[Request, Response] {

  def this(
    _compressionLevel: Int,
    _maxRequestSize: StorageUnit,
    _maxResponseSize: StorageUnit,
    _decompressionEnabled: Boolean,
    _channelBufferUsageTracker: Option[ChannelBufferUsageTracker],
    _enableTracing: Boolean,
    _maxInitialLineLength: StorageUnit,
    _maxHeaderSize: StorageUnit,
    _streaming: Boolean
  ) =
    this(
      _compressionLevel,
      _maxRequestSize,
      _maxResponseSize,
      _decompressionEnabled,
      _channelBufferUsageTracker,
      _enableTracing,
      _maxInitialLineLength,
      _maxHeaderSize,
      _streaming,
      NullStatsReceiver
    )

  require(
    _maxRequestSize < 2.gigabytes,
    s"maxRequestSize should be less than 2 Gb, but was ${_maxRequestSize}"
  )

  require(
    _maxResponseSize < 2.gigabytes,
    s"maxResponseSize should be less than 2 Gb, but was ${_maxResponseSize}"
  )

  private[this] final def HttpCodecName = "httpCodec"

  def compressionLevel(level: Int) = copy(_compressionLevel = level)
  def maxRequestSize(bufferSize: StorageUnit) = copy(_maxRequestSize = bufferSize)
  def maxResponseSize(bufferSize: StorageUnit) = copy(_maxResponseSize = bufferSize)
  def decompressionEnabled(yesno: Boolean) = copy(_decompressionEnabled = yesno)
  @deprecated("Use maxRequestSize to enforce buffer footprint limits", "2016-05-10")
  def channelBufferUsageTracker(usageTracker: ChannelBufferUsageTracker) =
    copy(_channelBufferUsageTracker = Some(usageTracker))
  def enableTracing(enable: Boolean) = copy(_enableTracing = enable)
  def maxInitialLineLength(length: StorageUnit) = copy(_maxInitialLineLength = length)
  def maxHeaderSize(size: StorageUnit) = copy(_maxHeaderSize = size)
  def streaming(enable: Boolean) = copy(_streaming = enable)

  def client = { config =>
    new Codec[Request, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          val maxInitialLineLengthInBytes = _maxInitialLineLength.inBytes.toInt
          val maxHeaderSizeInBytes = _maxHeaderSize.inBytes.toInt
          val maxChunkSize = 8192
          pipeline.addLast(
            HttpCodecName,
            new HttpClientCodec(maxInitialLineLengthInBytes, maxHeaderSizeInBytes, maxChunkSize)
          )

          if (!_streaming) {
            pipeline
              .addLast("httpDechunker", new HttpChunkAggregator(_maxResponseSize.inBytes.toInt))
            pipeline.addLast("clientExceptionMapper", ClientExceptionMapper)
          }

          if (_decompressionEnabled)
            pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

          pipeline
        }
      }

      override def prepareServiceFactory(
        underlying: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] =
        underlying.map(new DelayedReleaseService(_))

      override def prepareConnFactory(
        underlying: ServiceFactory[Request, Response],
        params: Stack.Params
      ): ServiceFactory[Request, Response] =
        // Note: This is a horrible hack to ensure that close() calls from
        // ExpiringService do not propagate until all chunks have been read
        // Waiting on CSL-915 for a proper fix.
        underlying.map { u =>
          val filters =
            new ClientContextFilter[Request, Response]
              .andThen(new DtabFilter.Injector)
              .andThenIf(
                !_streaming ->
                  new PayloadSizeFilter[Request, Response](
                    params[finagle.param.Stats].statsReceiver,
                    _.content.length,
                    _.content.length
                  )
              )

          filters.andThen(new DelayedReleaseService(u))
        }

      override def newClientTransport(
        ch: Channel,
        statsReceiver: StatsReceiver
      ): Transport[Any, Any] =
        super.newClientTransport(ch, statsReceiver)

      override def newClientDispatcher(transport: Transport[Any, Any], params: Stack.Params) =
        new HttpClientDispatcher(
          new HttpTransport(new Netty3ClientStreamTransport(transport)),
          params[finagle.param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
        )

      override def newTraceInitializer =
        if (_enableTracing) new HttpClientTraceInitializer[Request, Response]
        else TraceInitializerFilter.empty[Request, Response]

      override def protocolLibraryName: String = HttpCodecFactory.this.protocolLibraryName
    }
  }

  def server = { config =>
    new Codec[Request, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          if (_channelBufferUsageTracker.isDefined) {
            pipeline.addLast(
              "channelBufferManager",
              new ChannelBufferManager(_channelBufferUsageTracker.get)
            )
          }

          val maxRequestSizeInBytes = _maxRequestSize.inBytes.toInt
          val maxInitialLineLengthInBytes = _maxInitialLineLength.inBytes.toInt
          val maxHeaderSizeInBytes = _maxHeaderSize.inBytes.toInt
          val safeCodec = new SafeHttpServerCodec(
            maxInitialLineLengthInBytes,
            maxHeaderSizeInBytes,
            maxRequestSizeInBytes /* maxChunkSize */
          )
          pipeline.addLast(HttpCodecName, safeCodec)

          if (_compressionLevel > 0) {
            pipeline.addLast("httpCompressor", new HttpContentCompressor(_compressionLevel))
          } else if (_compressionLevel == -1) {
            pipeline.addLast("httpCompressor", new Netty3TextualContentCompressor)
          }

          if (_decompressionEnabled)
            pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

          // The payload size handler should come before the RespondToExpectContinue handler so that we don't
          // send a 100 CONTINUE for oversize requests we have no intention of handling.
          pipeline.addLast("payloadSizeHandler", new PayloadSizeHandler(maxRequestSizeInBytes))

          // Response to ``Expect: Continue'' requests.
          pipeline.addLast("respondToExpectContinue", new RespondToExpectContinue)
          if (!_streaming)
            pipeline
              .addLast("httpDechunker", new SafeServerHttpChunkAggregator(maxRequestSizeInBytes))

          pipeline
        }
      }

      override def newServerDispatcher(
        transport: Transport[Any, Any],
        service: Service[Request, Response]
      ): Closable =
        new HttpServerDispatcher(
          new HttpTransport(new Netty3ServerStreamTransport(transport)),
          service,
          ServerStatsReceiver
        )

      override def prepareConnFactory(
        underlying: ServiceFactory[Request, Response],
        params: Stack.Params
      ): ServiceFactory[Request, Response] = {
        val finagle.param.Stats(stats) = params[finagle.param.Stats]
        HttpNackFilter
          .newFilter(stats)
          .andThen(new DtabFilter.Extractor)
          .andThen(new ServerContextFilter[Request, Response])
          .andThenIf(
            !_streaming -> new PayloadSizeFilter[Request, Response](
              stats,
              _.content.length,
              _.content.length
            )
          )
          .andThen(underlying)
      }

      override def newTraceInitializer =
        if (_enableTracing) new HttpServerTraceInitializer[Request, Response]
        else TraceInitializerFilter.empty[Request, Response]
    }
  }

  override val protocolLibraryName: String = "http"
}
