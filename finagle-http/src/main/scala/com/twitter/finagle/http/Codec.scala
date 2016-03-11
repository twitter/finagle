package com.twitter.finagle.http














              "httpDechunker",
              maxInitialLineLengthInBytes, maxHeaderSizeInBytes, maxChunkSize))
              new HttpChunkAggregator(_maxResponseSize.inBytes.toInt))
            "httpCodec", new HttpClientCodec(
            pipeline.addLast(
            pipeline.addLast("httpDecompressor", new HttpContentDecompressor)
          channel, BadHttpRequest(ex), channel.getRemoteAddress()))
          if (!_streaming)
          if (_decompressionEnabled)
          pipeline
          pipeline.addLast(
          val maxChunkSize = 8192
          val maxHeaderSizeInBytes = _maxHeaderSize.inBytes.toInt
          val maxInitialLineLengthInBytes = _maxInitialLineLength.inBytes.toInt
          val pipeline = Channels.pipeline()
        ctx.sendUpstream(new UpstreamMessageEvent(
        def getPipeline() = {
        val channel = ctx.getChannel()
        }
      _annotateCipherHeader,
      _channelBufferUsageTracker,
      _compressionLevel,
      _decompressionEnabled,
      _enableTracing,
      _maxHeaderSize,
      _maxInitialLineLength,
      _maxRequestSize,
      _maxResponseSize,
      _streaming,
      case ex: Exception =>
      def pipelineFactory = new ChannelPipelineFactory {
      NullStatsReceiver)
      }
     super.handleUpstream(ctx, e)
    // rescues exceptions from the upstream handlers and calls notifyHandlerException(),
    // this only catches Codec exceptions -- when a handler calls sendUpStream(), it
    // which doesn't throw exceptions.
    _annotateCipherHeader: Option[String] = None,
    _annotateCipherHeader: Option[String],
    _channelBufferUsageTracker: Option[ChannelBufferUsageTracker] = None,
    _channelBufferUsageTracker: Option[ChannelBufferUsageTracker],
    _compressionLevel: Int = -1,
    _compressionLevel: Int,
    _decompressionEnabled: Boolean = true,
    _decompressionEnabled: Boolean,
    _enableTracing: Boolean = false,
    _enableTracing: Boolean,
    _maxHeaderSize: StorageUnit = 8192.bytes,
    _maxHeaderSize: StorageUnit,
    _maxInitialLineLength: StorageUnit = 4096.bytes,
    _maxInitialLineLength: StorageUnit,
    _maxRequestSize: StorageUnit = 5.megabytes,
    _maxRequestSize: StorageUnit,
    _maxResponseSize: StorageUnit = 5.megabytes,
    _maxResponseSize: StorageUnit,
    _statsReceiver: StatsReceiver = NullStatsReceiver
    _streaming: Boolean
    _streaming: Boolean = false,
    copy(_channelBufferUsageTracker = Some(usageTracker))
    maxChunkSize: Int)
    maxHeaderSize: Int,
    maxInitialLineLength: Int,
    new BadHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/bad-http-request", exception)
    new Codec[Request, Response] {
    s"maxRequestSize should be less than 2 Gb, but was ${_maxRequestSize}")
    s"maxResponseSize should be less than 2 Gb, but was ${_maxResponseSize}")
    this(
    try {
    }
    } catch {
  ) =
  def annotateCipherHeader(headerName: String) = copy(_annotateCipherHeader = Option(headerName))
  def apply(exception: Exception) =
  def channelBufferUsageTracker(usageTracker: ChannelBufferUsageTracker) =
  def client = { config =>
  def compressionLevel(level: Int) = copy(_compressionLevel = level)
  def decompressionEnabled(yesno: Boolean) = copy(_decompressionEnabled = yesno)
  def enableTracing(enable: Boolean) = copy(_enableTracing = enable)
  def maxHeaderSize(size: StorageUnit) = copy(_maxHeaderSize = size)
  def maxInitialLineLength(length: StorageUnit) = copy(_maxInitialLineLength = length)
  def maxRequestSize(bufferSize: StorageUnit) = copy(_maxRequestSize = bufferSize)
  def maxResponseSize(bufferSize: StorageUnit) = copy(_maxResponseSize = bufferSize)
  def streaming(enable: Boolean) = copy(_streaming = enable)
  def this(
  extends DefaultHttpRequest(httpVersion, method, uri)
  extends HttpServerCodec(maxInitialLineLength, maxHeaderSize, maxChunkSize)
  httpVersion: HttpVersion, method: HttpMethod, uri: String, exception: Exception)
  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
  require(_maxRequestSize < 2.gigabytes,
  require(_maxResponseSize < 2.gigabytes,
  }
 *
 *
 *
 * @param _compressionLevel The compression level to use. If passed the default value (-1) then use
 * @param _maxRequestSize The maximum size of the inbound request an HTTP server constructed with
 * @param _maxResponseSize The maximum size of the inbound response an HTTP client constructed with
 * @param _streaming Streaming allows applications to work with HTTP messages
 * [[com.twitter.finagle.http.codec.TextualContentCompressor TextualContentCompressor]] which will
 * [[org.jboss.netty.handler.codec.http.HttpContentCompressor HttpContentCompressor]] for all
 * `Int.MaxValue` bytes). Use streaming/chunked requests to handle larger messages.
 * `Int.MaxValue` bytes). Use streaming/chunked requests to handle larger messages.
 * `true`, the message content is available through a [[com.twitter.io.Reader]],
 * compress text-like content-types with the default compression level (6). Otherwise, use
 * content-types with specified compression level.
 * entire message content is buffered into a [[com.twitter.io.Buf]].
 * that have large (or infinite) content bodies. When this flag is set to
 * this codec can receive (default is 5 megabytes). Should be less than 2 gigabytes (up to
 * this codec can receive (default is 5 megabytes). Should be less than 2 gigabytes (up to
 * which gives the application a handle to the byte stream. If `false`, the
 */
) extends CodecFactory[Request, Response] {
/**
/** Convert exceptions to BadHttpRequests */
case class Http(
class SafeHttpServerCodec(
import com.twitter.conversions.storage._
import com.twitter.finagle._
import com.twitter.finagle.dispatch.{GenSerialClientDispatcher, ServerDispatcherInitializer}
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.http.codec._
import com.twitter.finagle.http.filter.{ClientContextFilter, DtabFilter, HttpNackFilter, ServerContextFilter}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, StorageUnit, Try}
import org.jboss.netty.channel.{Channel, ChannelEvent, ChannelHandlerContext, ChannelPipelineFactory, Channels, UpstreamMessageEvent}
import org.jboss.netty.handler.codec.http._
object BadHttpRequest {
private[finagle] case class BadHttpRequest(
{
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
            new ClientContextFilter[Request, Response].andThenIf(!_streaming ->
              new PayloadSizeFilter[Request, Response](
                params[param.Stats].statsReceiver, _.content.length, _.content.length
              )
            )

          filters.andThen(new DelayedReleaseService(u))
        }

      override def newClientTransport(ch: Channel, statsReceiver: StatsReceiver): Transport[Any,Any] =
        new HttpTransport(super.newClientTransport(ch, statsReceiver))

      override def newClientDispatcher(transport: Transport[Any, Any], params: Stack.Params) =
        new HttpClientDispatcher(
          transport,
          params[param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
        )

      override def newTraceInitializer =
        if (_enableTracing) new HttpClientTraceInitializer[Request, Response]
        else TraceInitializerFilter.empty[Request, Response]
    }
  }

  def server = { config =>
    new Codec[Request, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          if (_channelBufferUsageTracker.isDefined) {
            pipeline.addLast(
              "channelBufferManager", new ChannelBufferManager(_channelBufferUsageTracker.get))
          }

          val maxRequestSizeInBytes = _maxRequestSize.inBytes.toInt
          val maxInitialLineLengthInBytes = _maxInitialLineLength.inBytes.toInt
          val maxHeaderSizeInBytes = _maxHeaderSize.inBytes.toInt
          pipeline.addLast("httpCodec", new SafeHttpServerCodec(maxInitialLineLengthInBytes, maxHeaderSizeInBytes, maxRequestSizeInBytes))

          if (_compressionLevel > 0) {
            pipeline.addLast("httpCompressor", new HttpContentCompressor(_compressionLevel))
          } else if (_compressionLevel == -1) {
            pipeline.addLast("httpCompressor", new TextualContentCompressor)
          }

          // The payload size handler should come before the RespondToExpectContinue handler so that we don't
          // send a 100 CONTINUE for oversize requests we have no intention of handling.
          pipeline.addLast("payloadSizeHandler", new PayloadSizeHandler(maxRequestSizeInBytes))

          // Response to ``Expect: Continue'' requests.
          pipeline.addLast("respondToExpectContinue", new RespondToExpectContinue)
          if (!_streaming)
            pipeline.addLast(
              "httpDechunker",
              new HttpChunkAggregator(maxRequestSizeInBytes))

          _annotateCipherHeader foreach { headerName: String =>
            pipeline.addLast("annotateCipher", new AnnotateCipher(headerName))
          }

          pipeline
        }
      }

      override def newServerDispatcher(
        transport: Transport[Any, Any],
        service: Service[Request, Response],
        init: ServerDispatcherInitializer
      ): Closable =
        new HttpServerDispatcher(new HttpTransport(transport), service, init)

      override def prepareConnFactory(
        underlying: ServiceFactory[Request, Response],
        params: Stack.Params
      ): ServiceFactory[Request, Response] = {
        val param.Stats(stats) = params[param.Stats]
        new HttpNackFilter(stats)
          .andThen(new DtabFilter.Finagle[Request])
          .andThen(new ServerContextFilter[Request, Response])
          .andThenIf(!_streaming -> new PayloadSizeFilter[Request, Response](
            stats, _.content.length, _.content.length)
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

object Http {
  def get() = new Http()
}

object HttpTracing {

  /**
   * HTTP headers used for tracing.
   *
   * See [[headers()]] for Java compatibility.
   */
  object Header {
    val TraceId = "X-B3-TraceId"
    val SpanId = "X-B3-SpanId"
    val ParentSpanId = "X-B3-ParentSpanId"
    val Sampled = "X-B3-Sampled"
    val Flags = "X-B3-Flags"

    val All = Seq(TraceId, SpanId, ParentSpanId, Sampled, Flags)
    val Required = Seq(TraceId, SpanId)
  }

  /** Java compatibility API for [[Header]]. */
  def headers(): Header.type = Header

  /**
   * Remove any parameters from url.
   */
  private[http] def stripParameters(uri: String): String = {
    uri.indexOf('?') match {
      case -1 => uri
      case n  => uri.substring(0, n)
    }
  }
}

private object TraceInfo {
  import HttpTracing._

  val requestToTraceId: (Any) => Option[TraceId] = (a: Any) => a match {
    case r: Request => traceIdFromRequest(r)
    case _          => None
  }
  val responseToTraceId: (Any) => Option[TraceId] = (a: Any) => a match {
    case r: Response => traceIdFromResponse(r)
    case _           => None
  }

  def letTraceIdFromRequestHeaders[R](request: Request)(f: => R): R = {
    val traceId = if (hasAllTracingHeaders(request)) {
      traceIdFromRequest(request)
    } else if (request.headers.contains(Header.Flags)) {
      // even if there are no id headers we want to get the debug flag
      // this is to allow developers to just set the debug flag to ensure their
      // trace is collected
      Some(Trace.nextId.copy(flags = getFlags(request)))
    } else {
      Some(Trace.nextId)
    }

    // remove so the header is not visible to users
    Header.All foreach { request.headers.remove(_) }

    traceId match {
      case Some(id) => Trace.letId(id) {
        traceRpc(request)
        f
      }
      case None =>
        traceRpc(request)
        f
      }
  }

  def setClientRequestHeaders(request: Request): Unit = {
    Header.All.foreach { request.headers.remove(_) }

    val traceId = Trace.id
    request.headers.add(Header.TraceId, traceId.traceId.toString)
    request.headers.add(Header.SpanId, traceId.spanId.toString)
    // no parent id set means this is the root span
    traceId._parentId.foreach { id =>
      request.headers.add(Header.ParentSpanId, id.toString)
    }
    // three states of sampled, yes, no or none (let the server decide)
    traceId.sampled.foreach { sampled =>
      request.headers.add(Header.Sampled, sampled.toString)
    }
    request.headers.add(Header.Flags, traceId.flags.toLong)
    traceRpc(request)
  }

  def traceRpc(request: Request): Unit = {
    if (Trace.isActivelyTracing) {
      Trace.recordRpc(request.getMethod.getName)
      Trace.recordBinary("http.uri", stripParameters(request.getUri))
    }
  }

  /**
   * Safely extract the flags from the header, if they exist. Otherwise return empty flag.
   */
  def getFlags(request: Request): Flags = {
    try {
      Flags(Option(request.headers.get(Header.Flags)).map(_.toLong).getOrElse(0L))
    } catch {
      case _: Throwable => Flags()
    }
  }

  def getRepFlags(response: Response): Flags = try {
    Flags(Option(response.headers.get(Header.Flags)).map(_.toLong).getOrElse(0L))
  } catch {
    case _: Throwable => Flags()
  }

  private[this] def traceIdFromRequest(req: Request): Option[TraceId] = {
    val spanId = SpanId.fromString(req.headers.get(Header.SpanId))
 
    spanId map { sid: SpanId =>
      val traceId      = SpanId.fromString(req.headers.get(Header.TraceId))
      val parentSpanId = SpanId.fromString(req.headers.get(Header.ParentSpanId))
      val sampled = Option(req.headers.get(Header.Sampled)) flatMap { sampled =>
        Try(sampled.toBoolean).toOption
      }

      val flags = getFlags(req)
      TraceId(traceId, parentSpanId, sid, sampled, flags)
    }
  }

  private[this] def traceIdFromResponse(rep: Response): Option[TraceId] = {
    val spanId = SpanId.fromString(rep.headers.get(Header.SpanId))
 
    spanId map { sid: SpanId =>
      val traceId      = SpanId.fromString(rep.headers.get(Header.TraceId))
      val parentSpanId = SpanId.fromString(rep.headers.get(Header.ParentSpanId))
      val sampled = Option(rep.headers.get(Header.Sampled)) flatMap { sampled =>
        Try(sampled.toBoolean).toOption
      }

      val flags = getRepFlags(rep)
      TraceId(traceId, parentSpanId, sid, sampled, flags)
    }
  }

  private[this] def hasAllTracingHeaders(request: Request): Boolean =
    Header.Required.forall { request.headers.contains(_) }
}

private[finagle] class HttpServerTraceInitializer[Req <: Request, Rep]
  extends Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
  val role = TraceInitializerFilter.role
  val description = "Initialize the tracing system with trace info from the incoming request"

  def make(_tracer: param.Tracer, next: ServiceFactory[Req, Rep]) = {
    val param.Tracer(tracer) = _tracer
    val traceInitializer = Filter.mk[Req, Rep, Req, Rep] { (req, svc) =>
      Trace.letTracer(tracer) {
        TraceInfo.letTraceIdFromRequestHeaders(req) { svc(req) }
      }
    }
    traceInitializer andThen next
  }
}

private[finagle] class HttpClientTraceInitializer[Req <: Request, Rep]
  extends Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
  val role = TraceInitializerFilter.role
  val description = "Sets the next TraceId and attaches trace information to the outgoing request"
  def make(_tracer: param.Tracer, next: ServiceFactory[Req, Rep]) = {
    val param.Tracer(tracer) = _tracer
    val traceInitializer = Filter.mk[Req, Rep, Req, Rep] { (req, svc) =>
      Trace.letTracerAndNextId(tracer) {
        TraceInfo.setClientRequestHeaders(req)
        svc(req)
      }
    }
    traceInitializer andThen next
  }
}

/**
 * Pass along headers with the required tracing information.
 */
private[finagle] class HttpClientTracingFilter[Req <: Request, Res](serviceName: String)
  extends SimpleFilter[Req, Res]
{

  def apply(request: Req, service: Service[Req, Res]) = {
    TraceInfo.setClientRequestHeaders(request)
    service(request)
  }
}

/**
 * Adds tracing annotations for each http request we receive.
 * Including uri, when request was sent and when it was received.
 */
private[finagle] class HttpServerTracingFilter[Req <: Request, Res](serviceName: String)
  extends SimpleFilter[Req, Res]
{
  def apply(request: Req, service: Service[Req, Res]) =
    TraceInfo.letTraceIdFromRequestHeaders(request) {
    service(request)
  }
}
