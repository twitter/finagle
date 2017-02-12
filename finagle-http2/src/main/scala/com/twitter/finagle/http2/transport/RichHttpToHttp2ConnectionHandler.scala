package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.transport.Http2ClientDowngrader.{Message, Rst}
import com.twitter.logging.Logger
import io.netty.channel.{ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2.Http2Exception.HeaderListSizeException
import io.netty.handler.codec.http2._
import io.netty.util.concurrent.PromiseCombiner
import scala.util.control.NonFatal

/**
 * An extension of HttpToHttp2ConnectionHandler that handles streaming
 * correctly, by handling [[Message]] instead of [[HttpObject]] directly.
 */
private[http2] class RichHttpToHttp2ConnectionHandler(
    dec: Http2ConnectionDecoder,
    enc: Http2ConnectionEncoder,
    initialSettings: Http2Settings)
  extends HttpToHttp2ConnectionHandler(dec, enc, initialSettings, false) {

  private[this] val log = Logger.get(getClass.getName)

  private[this] def handleMessage(
    ctx: ChannelHandlerContext,
    promise: ChannelPromise,
    msg: HttpObject,
    streamId: Int
  ): Unit = {
    val combiner = new PromiseCombiner()

    try {
      msg match {
        case req: HttpRequest =>
          val headers = HttpConversionUtil.toHttp2Headers(req, false /* validateHeaders */)
          val endStream = req match {
            case full: FullHttpRequest if !full.content.isReadable => true
            case _ => false
          }
          val p = ctx.newPromise()
          combiner.add(p)

          val http1Headers = req.headers
          val dependencyId = http1Headers.getInt(
            HttpConversionUtil.ExtensionHeaderNames.STREAM_DEPENDENCY_ID.text, 0)

          val weight = http1Headers.getShort(
            HttpConversionUtil.ExtensionHeaderNames.STREAM_WEIGHT.text,
            Http2CodecUtil.DEFAULT_PRIORITY_WEIGHT)

          encoder.writeHeaders(
            ctx, streamId, headers, dependencyId, weight, false /* exclusive */, 0, endStream, p)
          // client can decide if a request is unhealthy immediately
          if (p.isDone && !p.isSuccess) {
            throw p.cause
          }
        case _ => // nop
      }
      msg match {
        case content: HttpContent =>
          val data = content.content
          val endStream = content.isInstanceOf[LastHttpContent]
          if (data.isReadable || (endStream && !content.isInstanceOf[HttpRequest])) {
            val p = ctx.newPromise()
            combiner.add(p)
            encoder.writeData(ctx, streamId, data, 0, endStream, p)
          }
        case _ => // nop
      }
    } catch {

      case e: Http2Exception =>
        val status = if (e.isInstanceOf[HeaderListSizeException])
          HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE
        else HttpResponseStatus.BAD_REQUEST
        val rep = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          status
        )
        ctx.fireChannelRead(Http2ClientDowngrader.Message(rep, streamId))
      case NonFatal(e) =>
        val p = ctx.newPromise()
        p.setFailure(e)
        combiner.add(p)
    } finally {
      combiner.finish(promise)
    }
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    msg match {
      case Message(obj, streamId) =>
        handleMessage(ctx, promise, obj, streamId)
      case Rst(streamId, errorCode) =>
        encoder.writeRstStream(ctx, streamId, errorCode, promise)
      // TODO we need to add support for writing RSTs and GOAWAYs
      case _ =>
        val wrongType = new IllegalArgumentException(
          s"Expected a Message, got ${msg.getClass.getName} instead.")
        log.error(wrongType, "Tried to write the wrong type to the http2 client pipeline")
        promise.setFailure(wrongType)
    }
  }
}
