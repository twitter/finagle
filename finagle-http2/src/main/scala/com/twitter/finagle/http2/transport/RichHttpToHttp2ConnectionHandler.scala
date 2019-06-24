package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.transport.StreamMessage._
import com.twitter.logging.Logger
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2.Http2Exception.HeaderListSizeException
import io.netty.handler.codec.http2._
import io.netty.util.ReferenceCountUtil
import io.netty.util.concurrent.PromiseCombiner
import scala.util.control.NonFatal

private object RichHttpToHttp2ConnectionHandler {
  private val log = Logger.get(getClass.getName)

  // We have a dedicated component that already validates all headers in our pipelines.
  // See com.twitter.finagle.netty4.http.handler.HeaderValidationHandler
  private final val NoHeaderValidation = false
  private final val NoPadding = 0
}

/**
 * An extension of HttpToHttp2ConnectionHandler that handles streaming
 * correctly, by handling [[Message]] instead of [[HttpObject]] directly.
 */
private[http2] class RichHttpToHttp2ConnectionHandler(
  dec: Http2ConnectionDecoder,
  enc: Http2ConnectionEncoder,
  initialSettings: Http2Settings,
  onActive: () => Unit)
    extends HttpToHttp2ConnectionHandler(dec, enc, initialSettings, false) {

  import RichHttpToHttp2ConnectionHandler._

  private[this] def writeHeaders(
    ctx: ChannelHandlerContext,
    streamId: Int,
    req: HttpRequest,
    endStream: Boolean,
    combiner: PromiseCombiner
  ): Unit = {
    val h1Headers = req.headers()
    val h2Headers = HttpConversionUtil.toHttp2Headers(req, NoHeaderValidation)
    val p = writeHeadersHelper(ctx, streamId, h1Headers, h2Headers, endStream, combiner)

    // client can decide if a request is unhealthy immediately
    if (p.isDone && !p.isSuccess) throw p.cause
  }

  private[this] def writeTrailers(
    ctx: ChannelHandlerContext,
    streamId: Int,
    h1Trailers: HttpHeaders,
    combiner: PromiseCombiner
  ): Unit = {
    val h2Trailers = HttpConversionUtil.toHttp2Headers(h1Trailers, NoHeaderValidation)
    writeHeadersHelper(ctx, streamId, h1Trailers, h2Trailers, true /*endStream*/, combiner)
  }

  private[this] def writeHeadersHelper(
    ctx: ChannelHandlerContext,
    streamId: Int,
    h1Headers: HttpHeaders,
    h2Headers: Http2Headers,
    endStream: Boolean,
    combiner: PromiseCombiner
  ): ChannelPromise = {
    val p = newPromise(ctx, combiner)

    encoder.writeHeaders(
      ctx,
      streamId,
      h2Headers,
      NoPadding,
      endStream,
      p
    )

    p
  }

  private[this] def newPromise(
    ctx: ChannelHandlerContext,
    combiner: PromiseCombiner
  ): ChannelPromise = {
    //
    // We register every created promise in a given PromiseCombiner as a single user-facing write
    // may correspond to multiple internal writes (e.g., headers + payload). Think of this as of
    // our very own Future.join.
    //
    val p = ctx.newPromise()
    combiner.add(p.asInstanceOf[io.netty.util.concurrent.Future[_]])
    p
  }

  private[this] def writeMessage(
    ctx: ChannelHandlerContext,
    promise: ChannelPromise,
    msg: HttpObject,
    streamId: Int
  ): Unit = {
    try {
      val combiner = new PromiseCombiner()
      msg match {
        case full: FullHttpRequest =>
          // Full HTTP requests might have everything, headers, payload, and trailers. We write them
          // in order.
          // - If both payload and trailers are empty, headers terminate the stream.
          // - If trailers are empty, the payload terminates the stream.
          // - Otherwise, trailers terminate the stream.
          val data = full.content
          val trailers = full.trailingHeaders
          writeHeaders(
            ctx,
            streamId,
            full,
            !data.isReadable && trailers.isEmpty /*endStream*/,
            combiner)

          if (data.isReadable) {
            encoder.writeData(
              ctx,
              streamId,
              data,
              NoPadding,
              trailers.isEmpty,
              newPromise(ctx, combiner))
          }

          if (!trailers.isEmpty) {
            writeTrailers(ctx, streamId, trailers, combiner)
          }

        case req: HttpRequest =>
          // Regular HTTP requests are just headers. They never terminate the stream.
          writeHeaders(ctx, streamId, req, false /*endStream*/, combiner)

        case last: LastHttpContent =>
          // The last chunk in the stream may additionally carry trailers (trailing headers).
          // Similar to a full HTTP request, we write payload and trailers in order. If trailers are
          // empty, payload terminates the stream.
          val data = last.content
          val trailers = last.trailingHeaders

          encoder.writeData(
            ctx,
            streamId,
            data,
            NoPadding,
            trailers.isEmpty /*endStream*/,
            newPromise(ctx, combiner))

          if (!trailers.isEmpty) {
            writeTrailers(ctx, streamId, trailers, combiner)
          }

        case chunk: HttpContent =>
          // Regular HTTP chunks are just data. They never terminate the stream.
          encoder.writeData(
            ctx,
            streamId,
            chunk.content,
            NoPadding,
            false /*endStream*/,
            newPromise(ctx, combiner))

        case _ =>
          failOnWrongMessageType("HttpRequest or HttpContent", msg, promise)
      }
      combiner.finish(promise)
    } catch {
      case e: Http2Exception =>
        val status =
          if (e.isInstanceOf[HeaderListSizeException])
            HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE
          else HttpResponseStatus.BAD_REQUEST

        val rep = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          status
        )

        ctx.fireChannelRead(Message(rep, streamId))
        promise.trySuccess()

      case NonFatal(e) =>
        promise.setFailure(e)
    }
  }

  private[this] def failOnWrongMessageType(expected: String, msg: Any, p: ChannelPromise): Unit = {
    val wrongType = new IllegalArgumentException(
      s"Expected a $expected, got ${msg.getClass.getName} instead."
    )
    ReferenceCountUtil.release(msg)
    log.error(wrongType, "Tried to write the wrong type to the HTTP/2 client pipeline")
    p.setFailure(wrongType)
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    msg match {
      case Message(obj, streamId) =>
        writeMessage(ctx, promise, obj, streamId)
      case Rst(streamId, errorCode) =>
        encoder.writeRstStream(ctx, streamId, errorCode, promise)
      case Ping =>
        encoder.writePing(ctx, false /* ack */, 0 /* data */, promise)
      case GoAway(obj, streamId, errorCode) =>
        ReferenceCountUtil.release(obj)
        encoder.writeGoAway(
          ctx,
          streamId,
          errorCode, /* debugData */ Unpooled.EMPTY_BUFFER,
          promise
        )

      case _ =>
        failOnWrongMessageType("StreamMessage", msg, promise)
    }
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    super.handlerAdded(ctx)
    onActive()
  }

  override def onStreamError(
    ctx: ChannelHandlerContext,
    outbound: Boolean,
    cause: Throwable,
    http2Ex: Http2Exception.StreamException
  ): Unit = {
    if (!outbound) http2Ex match {
      case ex: HeaderListSizeException if ex.duringDecode =>
        ctx.fireChannelRead(StreamException(ex, ex.streamId))
      case _ => // nop
    }

    super.onStreamError(ctx, outbound, cause, http2Ex)
  }
}
