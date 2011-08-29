package com.twitter.finagle.http

/**
 * This puts it all together: The HTTP codec itself.
 */

import org.jboss.netty.channel.{
  Channels, ChannelEvent, ChannelHandlerContext, ChannelPipelineFactory, UpstreamMessageEvent}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import com.twitter.util.{StorageUnit, Future}
import com.twitter.conversions.storage._

import com.twitter.finagle.{Codec, CodecFactory, CodecException, Service, SimpleFilter}
import com.twitter.finagle.http.codec._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}


private[http] class BadHttpRequest(httpVersion: HttpVersion, method: HttpMethod, uri: String, codecError: String)
  extends DefaultHttpRequest(httpVersion, method, uri)

private[http] object BadHttpRequest {
  def apply(codecError: String) =
    new BadHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/bad-http-request", codecError)
}

/** Convert exceptions to BadHttpRequests */
private[http] class SafeHttpServerCodec(
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
    _maxRequestSize: StorageUnit = 1.megabyte,
    _maxResponseSize: StorageUnit = 1.megabyte,
    _decompressionEnabled: Boolean = true,
    _channelBufferUsageTracker: Option[ChannelBufferUsageTracker] = None,
    _annotateCipherHeader: Option[String] = None)
  extends CodecFactory[HttpRequest, HttpResponse]
{
  def compressionLevel(level: Int) = copy(_compressionLevel = level)
  def maxRequestSize(bufferSize: StorageUnit) = copy(_maxRequestSize = bufferSize)
  def maxResponseSize(bufferSize: StorageUnit) = copy(_maxResponseSize = bufferSize)
  def decompressionEnabled(yesno: Boolean) = copy(_decompressionEnabled = yesno)
  def channelBufferUsageTracker(usageTracker: ChannelBufferUsageTracker) =
    copy(_channelBufferUsageTracker = Some(usageTracker))
  def annotateCipherHeader(headerName: String) = copy(_annotateCipherHeader = Option(headerName))

  def client = Function.const {
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

          pipeline.addLast(
            "connectionLifecycleManager",
            new ClientConnectionManager)

          pipeline
        }
      }
    }
  }

  def server = Function.const {
    new Codec[HttpRequest, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          if (_channelBufferUsageTracker.isDefined) {
            pipeline.addLast(
              "channelBufferManager", new ChannelBufferManager(_channelBufferUsageTracker.get))
          }

          val maxRequestSizeInBytes = _maxRequestSize.inBytes.toInt
          if (maxRequestSizeInBytes < 8192) {
            pipeline.addLast("httpCodec", new SafeHttpServerCodec(4096, 8192, maxRequestSizeInBytes))
          } else {
            pipeline.addLast("httpCodec", new SafeHttpServerCodec(4096, 8192, 8192))
          }

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

      override def prepareService(
        underlying: Service[HttpRequest, HttpResponse]
      ): Future[Service[HttpRequest, HttpResponse]] = {
        val checkRequest = new CheckHttpRequestFilter
        Future.value(checkRequest andThen underlying)
      }
    }
  }
}

object Http {
  def get() = new Http()
}


/**
 * Http codec for rich Request/Response objects.
 * Note the ValidateRequestFilter isn't baked in, as in the Http Codec.
 */
case class RichHttp[REQUEST <: Request](
     httpFactory: CodecFactory[HttpRequest, HttpResponse])
  extends CodecFactory[REQUEST, Response] {

  def client = Function.const {
    new Codec[REQUEST, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = httpFactory.client(null).pipelineFactory.getPipeline()
          pipeline.addLast("requestDecoder", new RequestDecoder)
          pipeline.addLast("responseEncoder", new ResponseEncoder)
          pipeline
        }
      }
    }
  }

  def server = Function.const {
    new Codec[REQUEST, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = httpFactory.server(null).pipelineFactory.getPipeline()
          pipeline.addLast("requestDecoder", new RequestDecoder)
          pipeline.addLast("responseEncoder", new ResponseEncoder)
          pipeline
        }
      }

      // prepareService is not defined
    }
  }
}
