package com.twitter.finagle.http

/**
 * This puts it all together: The HTTP codec itself.
 */

import com.twitter.conversions.storage._
import com.twitter.finagle._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.http.codec._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.util.{Try, StorageUnit, Future}
import java.net.InetSocketAddress
import org.jboss.netty.channel.{
  ChannelPipelineFactory, UpstreamMessageEvent, Channels,
  ChannelEvent, ChannelHandlerContext, SimpleChannelDownstreamHandler, MessageEvent}
import org.jboss.netty.handler.codec.http._

case class BadHttpRequest(httpVersion: HttpVersion, method: HttpMethod, uri: String, codecError: String)
  extends DefaultHttpRequest(httpVersion, method, uri)

object BadHttpRequest {
  def apply(codecError: String) =
    new BadHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/bad-http-request", codecError)
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
          channel, BadHttpRequest(ex.toString()), channel.getRemoteAddress()))
    }
  }
}

/* Respond to BadHttpRequests with 400 errors */
abstract class CheckRequestFilter[Req](
    statsReceiver: StatsReceiver = NullStatsReceiver)
  extends SimpleFilter[Req, HttpResponse]
{
  private[this] val badRequestCount = statsReceiver.counter("bad_requests")

  private[this] val BadRequestResponse =
    Response(HttpVersion.HTTP_1_0, HttpResponseStatus.BAD_REQUEST)

  def apply(request: Req, service: Service[Req, HttpResponse]) = {
    toHttpRequest(request) match {
      case httpRequest: BadHttpRequest =>
        badRequestCount.incr()
        // BadRequstResponse will cause ServerConnectionManager to close the channel.
        Future.value(BadRequestResponse)
      case _ =>
        service(request)
    }
  }

  def toHttpRequest(request: Req): HttpRequest
}

private[this] class CheckHttpRequestFilter extends CheckRequestFilter[HttpRequest]
{
  val BadRequestResponse =
    Response(HttpVersion.HTTP_1_0, HttpResponseStatus.BAD_REQUEST)


  def toHttpRequest(request: HttpRequest) = request
}


case class Http(
    _compressionLevel: Int = 0,
    _maxRequestSize: StorageUnit = 5.megabytes,
    _maxResponseSize: StorageUnit = 5.megabytes,
    _decompressionEnabled: Boolean = true,
    _channelBufferUsageTracker: Option[ChannelBufferUsageTracker] = None,
    _annotateCipherHeader: Option[String] = None,
    _enableTracing: Boolean = false,
    _maxInitialLineLength: StorageUnit = 4096.bytes,
    _maxHeaderSize: StorageUnit = 8192.bytes)
  extends CodecFactory[HttpRequest, HttpResponse]
{
  def compressionLevel(level: Int) = copy(_compressionLevel = level)
  def maxRequestSize(bufferSize: StorageUnit) = copy(_maxRequestSize = bufferSize)
  def maxResponseSize(bufferSize: StorageUnit) = copy(_maxResponseSize = bufferSize)
  def decompressionEnabled(yesno: Boolean) = copy(_decompressionEnabled = yesno)
  def channelBufferUsageTracker(usageTracker: ChannelBufferUsageTracker) =
    copy(_channelBufferUsageTracker = Some(usageTracker))
  def annotateCipherHeader(headerName: String) = copy(_annotateCipherHeader = Option(headerName))
  def enableTracing(enable: Boolean) = copy(_enableTracing = enable)
  def maxInitialLineLength(length: StorageUnit) = copy(_maxInitialLineLength = length)
  def maxHeaderSize(size: StorageUnit) = copy(_maxHeaderSize = size)

  def client = { config =>
    new Codec[HttpRequest, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpClientCodec())
          pipeline.addLast(
            "httpDechunker",
            new HttpChunkAggregator(_maxResponseSize.inBytes.toInt))

          if (_decompressionEnabled)
            pipeline.addLast("httpDecompressor", new HttpContentDecompressor)

          pipeline
        }
      }

      override def prepareConnFactory(
        underlying: ServiceFactory[HttpRequest, HttpResponse]
      ): ServiceFactory[HttpRequest, HttpResponse] =
        if (_enableTracing) {
          new HttpClientTracingFilter[HttpRequest, HttpResponse](config.serviceName) andThen
            super.prepareConnFactory(underlying)
        } else
          super.prepareConnFactory(underlying)

      override def newClientDispatcher(transport: Transport[HttpRequest, HttpResponse]) =
        new HttpClientDispatcher(transport)
    }
  }

  def server = { config =>
    new Codec[HttpRequest, HttpResponse] {
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
            pipeline.addLast(
              "httpCompressor",
              new HttpContentCompressor(_compressionLevel))
          }

          // Response to ``Expect: Continue'' requests.
          pipeline.addLast("respondToExpectContinue", new RespondToExpectContinue)
          pipeline.addLast(
            "httpDechunker",
            new HttpChunkAggregator(maxRequestSizeInBytes))

          _annotateCipherHeader foreach { headerName: String =>
            pipeline.addLast("annotateCipher", new AnnotateCipher(headerName))
          }

          pipeline.addLast(
            "connectionLifecycleManager",
            new ServerConnectionManager)

          pipeline
        }
      }

      override def prepareConnFactory(
        underlying: ServiceFactory[HttpRequest, HttpResponse]
      ): ServiceFactory[HttpRequest, HttpResponse] = {
        val checkRequest = new CheckHttpRequestFilter
        if (_enableTracing) {
          val tracingFilter = new HttpServerTracingFilter[HttpRequest, HttpResponse](
            config.serviceName,
            config.boundInetSocketAddress
          )
          tracingFilter andThen checkRequest andThen underlying
        } else {
          checkRequest andThen underlying
        }
      }
    }
  }
}

object Http {
  def get() = new Http()
}

object HttpTracing {
  object Header {
    val TraceId = "X-B3-TraceId"
    val SpanId = "X-B3-SpanId"
    val ParentSpanId = "X-B3-ParentSpanId"
    val Sampled = "X-B3-Sampled"
    val Flags = "X-B3-Flags"

    val All = Seq(TraceId, SpanId, ParentSpanId, Sampled, Flags)
    val Required = Seq(TraceId, SpanId)
  }

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

/**
 * Pass along headers with the required tracing information.
 */
class HttpClientTracingFilter[Req <: HttpRequest, Res](serviceName: String)
  extends SimpleFilter[Req, Res]
{
  import HttpTracing._

  def apply(request: Req, service: Service[Req, Res]) = Trace.unwind {
    Header.All foreach { request.removeHeader(_) }

    request.addHeader(Header.TraceId, Trace.id.traceId.toString)
    request.addHeader(Header.SpanId, Trace.id.spanId.toString)
    // no parent id set means this is the root span
    Trace.id._parentId foreach { id =>
      request.addHeader(Header.ParentSpanId, id.toString)
    }
    // three states of sampled, yes, no or none (let the server decide)
    Trace.id.sampled foreach { sampled =>
      request.addHeader(Header.Sampled, sampled.toString)
    }
    request.addHeader(Header.Flags, Trace.id.flags.toLong)

    if (Trace.isActivelyTracing) {
      Trace.recordRpcname(serviceName, request.getMethod.getName)
      Trace.recordBinary("http.uri", stripParameters(request.getUri))

      Trace.record(Annotation.ClientSend())
      service(request) map { response =>
        Trace.record(Annotation.ClientRecv())
        response
      }
    }
    else
      service(request)
  }
}

/**
 * Adds tracing annotations for each http request we receive.
 * Including uri, when request was sent and when it was received.
 */
class HttpServerTracingFilter[Req <: HttpRequest, Res](serviceName: String, boundAddress: InetSocketAddress)
  extends SimpleFilter[Req, Res]
{
  import HttpTracing._

  def apply(request: Req, service: Service[Req, Res]) = Trace.unwind {

    if (Header.Required.forall { request.containsHeader(_) }) {
      val spanId = SpanId.fromString(request.getHeader(Header.SpanId))

      spanId foreach { sid =>
        val traceId = SpanId.fromString(request.getHeader(Header.TraceId))
        val parentSpanId = SpanId.fromString(request.getHeader(Header.ParentSpanId))

        val sampled = Option(request.getHeader(Header.Sampled)) flatMap { sampled =>
          Try(sampled.toBoolean).toOption
        }

        val flags = getFlags(request)
        Trace.setId(TraceId(traceId, parentSpanId, sid, sampled, flags))
      }
    } else if (request.containsHeader(Header.Flags)) {
      // even if there are no id headers we want to get the debug flag
      // this is to allow developers to just set the debug flag to ensure their
      // trace is collected
      Trace.setId(Trace.id.copy(flags = getFlags(request)))
    }

    // remove so the header is not visible to users
    Header.All foreach { request.removeHeader(_) }

    // even if no trace id was passed from the client we log the annotations
    // with a locally generated id
    if (Trace.isActivelyTracing) {
      Trace.recordRpcname(serviceName, request.getMethod.getName)
      Trace.recordBinary("http.uri", stripParameters(request.getUri))

      Trace.record(Annotation.ServerRecv())
      service(request) map { response =>
        Trace.record(Annotation.ServerSend())
        response
      }
    }
    else
      service(request)
  }

  /**
   * Safely extract the flags from the header, if they exist. Otherwise return empty flag.
   */
  def getFlags(request: Req): Flags = {
    try {
      Flags(Option(request.getHeader(Header.Flags)).map(_.toLong).getOrElse(0L))
    } catch {
      case _ => Flags()
    }
  }
}

/**
 * Http codec for rich Request/Response objects.
 * Note the CheckHttpRequestFilter isn't baked in, as in the Http Codec.
 */
case class RichHttp[REQUEST <: Request](
     httpFactory: Http)
  extends CodecFactory[REQUEST, Response] {

  def client = { config =>
    new Codec[REQUEST, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = httpFactory.client(null).pipelineFactory.getPipeline()
          pipeline.addLast("requestDecoder", new RequestEncoder)
          pipeline.addLast("responseEncoder", new ResponseDecoder)
          pipeline
        }
      }

      override def prepareConnFactory(
        underlying: ServiceFactory[REQUEST, Response]
      ): ServiceFactory[REQUEST, Response] =
        if (httpFactory._enableTracing)
          new HttpClientTracingFilter[REQUEST, Response](config.serviceName) andThen underlying
        else
          underlying

      override def newClientDispatcher(transport: Transport[REQUEST, Response]) =
        new HttpClientDispatcher(transport)
    }
  }

  def server = { config =>
    new Codec[REQUEST, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = httpFactory.server(null).pipelineFactory.getPipeline()
          pipeline.addLast("serverRequestDecoder", new RequestDecoder)
          pipeline.addLast("serverResponseEncoder", new ResponseEncoder)
          pipeline
        }
      }

      override def prepareConnFactory(
        underlying: ServiceFactory[REQUEST, Response]
      ): ServiceFactory[REQUEST, Response] =
        if (httpFactory._enableTracing) {
          val tracingFilter = new HttpServerTracingFilter[REQUEST, Response](config.serviceName, config.boundInetSocketAddress)
          tracingFilter andThen underlying
        } else {
          underlying
        }
    }
  }
}
