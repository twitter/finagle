package com.twitter.finagle.http

import com.twitter.conversions.storage._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.http.codec._
import com.twitter.finagle.http.filter.{ClientContextFilter, DtabFilter, HttpNackFilter, ServerContextFilter}
import com.twitter.finagle.http.netty.{Netty3ClientStreamTransport, Netty3ServerStreamTransport}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver, ServerStatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{NonFatal, Closable, StorageUnit, Try}
import java.net.InetSocketAddress
import org.jboss.netty.channel.{Channel, ChannelEvent, ChannelHandlerContext, ChannelPipelineFactory, Channels, UpstreamMessageEvent}
import org.jboss.netty.handler.codec.http._

private[finagle] case class BadHttpRequest(
  httpVersion: HttpVersion, method: HttpMethod, uri: String, exception: Throwable)
    extends DefaultHttpRequest(httpVersion, method, uri)

object BadHttpRequest {
  def apply(exception: Throwable): BadHttpRequest =
    new BadHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/bad-http-request", exception)
}

private[finagle] sealed trait BadReq
private[finagle] trait ContentTooLong extends BadReq
private[finagle] trait UriTooLong extends BadReq
private[finagle] trait HeaderFieldsTooLarge extends BadReq

private[http] case class BadRequest(httpRequest: HttpRequest, exception: Throwable)
  extends Request with BadReq {
  lazy val remoteSocketAddress = new InetSocketAddress(0)
}

private[finagle] object BadRequest {

  def apply(msg: BadHttpRequest): BadRequest =
    new BadRequest(msg, msg.exception)

  def apply(exn: Throwable): BadRequest = {
    val msg = new BadHttpRequest(
      HttpVersion.HTTP_1_0,
      HttpMethod.GET,
      "/bad-http-request",
      exn
    )

    apply(msg)
  }

  def contentTooLong(msg: BadHttpRequest): BadRequest with ContentTooLong =
    new BadRequest(msg, msg.exception) with ContentTooLong

  def contentTooLong(exn: Throwable): BadRequest with ContentTooLong = {
    val msg = new BadHttpRequest(
      HttpVersion.HTTP_1_0,
      HttpMethod.GET,
      "/bad-http-request",
      exn
    )
    contentTooLong(msg)
  }

  def uriTooLong(msg: BadHttpRequest): BadRequest with UriTooLong =
    new BadRequest(msg, msg.exception) with UriTooLong

  def uriTooLong(exn: Throwable): BadRequest with UriTooLong = {
    val msg = new BadHttpRequest(
      HttpVersion.HTTP_1_0,
      HttpMethod.GET,
      "/bad-http-request",
      exn
    )
    uriTooLong(msg)
  }

  def headerTooLong(msg: BadHttpRequest): BadRequest with HeaderFieldsTooLarge  =
    new BadRequest(msg, msg.exception) with HeaderFieldsTooLarge

  def headerTooLong(exn: Throwable): BadRequest with HeaderFieldsTooLarge = {
    val msg = new BadHttpRequest(
      HttpVersion.HTTP_1_0,
      HttpMethod.GET,
      "/bad-http-request",
      exn
    )
    headerTooLong(msg)
  }
}


/**
 * a HttpChunkAggregator which recovers decode failures into 4xx http responses
 */
private[http] class SafeServerHttpChunkAggregator(maxContentSizeBytes: Int) extends HttpChunkAggregator(maxContentSizeBytes) {

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent): Unit = {
    try {
      super.handleUpstream(ctx, e)
    } catch {
      case NonFatal(ex) =>
        val channel = ctx.getChannel()
        ctx.sendUpstream(new UpstreamMessageEvent(
          channel, BadHttpRequest(ex), channel.getRemoteAddress()))
    }
  }
}

/** Convert exceptions to BadHttpRequests */
class SafeHttpServerCodec(
    maxInitialLineLength: Int,
    maxHeaderSize: Int,
    maxChunkSize: Int)
  extends HttpServerCodec(maxInitialLineLength, maxHeaderSize, maxChunkSize)
{
  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    // this only catches Codec exceptions -- when a handler calls sendUpStream(), it
    // rescues exceptions from the upstream handlers and calls notifyHandlerException(),
    // which doesn't throw exceptions.
    try {
     super.handleUpstream(ctx, e)
    } catch {
      case ex: Exception =>
        val channel = ctx.getChannel()
        ctx.sendUpstream(new UpstreamMessageEvent(
          channel, BadHttpRequest(ex), channel.getRemoteAddress()))
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
case class Http(
    _compressionLevel: Int = -1,
    _maxRequestSize: StorageUnit = 5.megabytes,
    _maxResponseSize: StorageUnit = 5.megabytes,
    _decompressionEnabled: Boolean = true,
    _channelBufferUsageTracker: Option[ChannelBufferUsageTracker] = None,
    _annotateCipherHeader: Option[String] = None,
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
    _annotateCipherHeader: Option[String],
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
      _annotateCipherHeader,
      _enableTracing,
      _maxInitialLineLength,
      _maxHeaderSize,
      _streaming,
      NullStatsReceiver)

  require(_maxRequestSize < 2.gigabytes,
    s"maxRequestSize should be less than 2 Gb, but was ${_maxRequestSize}")

  require(_maxResponseSize < 2.gigabytes,
    s"maxResponseSize should be less than 2 Gb, but was ${_maxResponseSize}")

  def compressionLevel(level: Int) = copy(_compressionLevel = level)
  def maxRequestSize(bufferSize: StorageUnit) = copy(_maxRequestSize = bufferSize)
  def maxResponseSize(bufferSize: StorageUnit) = copy(_maxResponseSize = bufferSize)
  def decompressionEnabled(yesno: Boolean) = copy(_decompressionEnabled = yesno)
  @deprecated("Use maxRequestSize to enforce buffer footprint limits", "2016-05-10")
  def channelBufferUsageTracker(usageTracker: ChannelBufferUsageTracker) =
    copy(_channelBufferUsageTracker = Some(usageTracker))
  def annotateCipherHeader(headerName: String) = copy(_annotateCipherHeader = Option(headerName))
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
            "httpCodec", new HttpClientCodec(
              maxInitialLineLengthInBytes, maxHeaderSizeInBytes, maxChunkSize))

          if (!_streaming)
            pipeline.addLast(
              "httpDechunker",
              new HttpChunkAggregator(_maxResponseSize.inBytes.toInt))

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
              .andThenIf(!_streaming ->
                new PayloadSizeFilter[Request, Response](
                  params[param.Stats].statsReceiver, _.content.length, _.content.length
                )
              )

          filters.andThen(new DelayedReleaseService(u))
        }

      override def newClientTransport(ch: Channel, statsReceiver: StatsReceiver): Transport[Any,Any] =
        super.newClientTransport(ch, statsReceiver)

      override def newClientDispatcher(transport: Transport[Any, Any], params: Stack.Params) =
        new HttpClientDispatcher(
          new HttpTransport(new Netty3ClientStreamTransport(transport)),
          params[param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
        )

      override def newTraceInitializer =
        if (_enableTracing) new HttpClientTraceInitializer[Request, Response]
        else TraceInitializerFilter.empty[Request, Response]

      override def protocolLibraryName: String = Http.this.protocolLibraryName
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

          if (_decompressionEnabled)
            pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

          // The payload size handler should come before the RespondToExpectContinue handler so that we don't
          // send a 100 CONTINUE for oversize requests we have no intention of handling.
          pipeline.addLast("payloadSizeHandler", new PayloadSizeHandler(maxRequestSizeInBytes))

          // Response to ``Expect: Continue'' requests.
          pipeline.addLast("respondToExpectContinue", new RespondToExpectContinue)
          if (!_streaming)
            pipeline.addLast(
              "httpDechunker",
              new SafeServerHttpChunkAggregator(maxRequestSizeInBytes))

          _annotateCipherHeader foreach { headerName: String =>
            pipeline.addLast("annotateCipher", new AnnotateCipher(headerName))
          }

          pipeline
        }
      }

      override def newServerDispatcher(
        transport: Transport[Any, Any],
        service: Service[Request, Response]
      ): Closable = new HttpServerDispatcher(
        new HttpTransport(new Netty3ServerStreamTransport(transport)),
        service,
        ServerStatsReceiver)

      override def prepareConnFactory(
        underlying: ServiceFactory[Request, Response],
        params: Stack.Params
      ): ServiceFactory[Request, Response] = {
        val param.Stats(stats) = params[param.Stats]
        new HttpNackFilter(stats)
          .andThen(new DtabFilter.Extractor)
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

  def letTraceIdFromRequestHeaders[R](request: Request)(f: => R): R = {
    val id = if (Header.Required.forall { request.headers.contains(_) }) {
      val spanId = SpanId.fromString(request.headers.get(Header.SpanId))

      spanId map { sid =>
        val traceId = SpanId.fromString(request.headers.get(Header.TraceId))
        val parentSpanId = SpanId.fromString(request.headers.get(Header.ParentSpanId))

        val sampled = Option(request.headers.get(Header.Sampled)) flatMap { sampled =>
          Try(sampled.toBoolean).toOption
        }

        val flags = getFlags(request)
        TraceId(traceId, parentSpanId, sid, sampled, flags)
      }
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

    id match {
      case Some(id) =>
        Trace.letId(id) {
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

private[finagle] object OpenTracing {

  import io.opentracing.Tracer
  import io.opentracing.Span
  import java.util.HashMap
  import scala.collection.JavaConverters._

  /**
   * Set the parent span of all spans created during the scope
   * of the enclosed function.
   */
  def letParentSpan[R](span: Span)(f: => R): R = {
    Contexts.local.let(spanCtx, span)(f)
  }

  /**
   * Set the operation name for all spans created during the scope
   * of the enclosed function.
   */
  def letOperationName[R](operationName: String)(f: => R): R = {
    Contexts.local.let(rpcCtx, operationName)(f)
  }

  private[this] val spanCtx = Contexts.local.newKey[Object]
  private[this] val rpcCtx = Contexts.local.newKey[String]

  /**
   * Creates a new span for the HTTP request and injects the necessary
   * parameters so the server can join the trace.
   * 
   * The parent span and operation name of the request can be set by
   * calling letParentSpan and letOperationName on the request
   *
   * @param tracer the OpenTracing tracer to create the span
   */
  class ClientFilter[Req <: Request, Res](tracer: Tracer)
    extends SimpleFilter[Req, Res]
  {
    def apply(request: Req, service: Service[Req, Res]) = {
      val rpcName = Contexts.local.getOrElse(rpcCtx, () => "Finagle HTTP Request")
      val span = createSpanFromContext(tracer, rpcName)
      var textMap = new HashMap[String,String]()

      // NOTE: OpenTracing uses `inject` to propagate arbitrary states
      // across process boundaries without tightly coupling to any particular
      // wire format or object type.
      tracer.inject(span, textMap)
      for((k:String, v:String) <- textMap.asScala) { request.headers.add(k, v) }

      // close the span once the response is received
      val response = service(request) onFailure { t: Throwable =>
        span.log(t.getMessage, null)
      } ensure {
        span.close()
      }
      response
    }
  }

  /**
   * Traces this HTTP response using the injected headers. Depending on the
   * OpenTracing implementation, this may continue the span from the
   * client request or create a new child span for the server response.
   * If there is no injected span to join, creates a new span from the
   * current span context.
   */
  class ServerFilter[Req <: Request, Res](tracer: Tracer)
    extends SimpleFilter[Req, Res]
  {
    def apply(request: Req, service: Service[Req, Res]) = {
      // extract the span tracking the client request
      var textMap: HashMap[String, String] = new HashMap[String, String]()
      for((k: String, v:String) <- request.headerMap) {
        textMap.put(k,v)
      }
      
      // NOTE: OpenTracing uses `join` as the complement to `inject`:
      // it uses a generic "carrier" / container to extract the trace
      // context and resume the trace on the server.
      val span: Span = try {
        tracer.join(textMap).withOperationName("serve").start()
      } catch {
        case _: Throwable => {
          createSpanFromContext(tracer, "serve").log("Failed to join span", null)
        }
      }
      val response = service(request) ensure {
        span.close()
      }
      response
    }
  }

  private[this] def createSpanFromContext(tracer: Tracer, description: String): Span = {
    val parentSpan = Contexts.local.getOrElse(spanCtx, () => None)
    var sb = tracer.buildSpan(description)
    if(parentSpan != None) {
      sb = sb.withParent(parentSpan.asInstanceOf[Span])
    }
    val span = sb.start()
    span
  }
}