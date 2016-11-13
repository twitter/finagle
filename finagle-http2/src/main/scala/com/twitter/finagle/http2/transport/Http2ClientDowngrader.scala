package com.twitter.finagle.http2.transport

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2.{Http2EventAdapter, HttpConversionUtil, Http2Headers,
  DefaultHttp2Headers, Http2Connection}
import java.nio.charset.StandardCharsets.UTF_8

/**
 * `Http2ClientDowngrader` wraps RSTs, GOAWAYs, HEADERS, and DATA in thin finagle wrappers.
 */
private[http2] class Http2ClientDowngrader(connection: Http2Connection) extends Http2EventAdapter {

  private[this] val headersKey: Http2Connection.PropertyKey = connection.newKey()

  import Http2ClientDowngrader._

  override def onDataRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    data: ByteBuf,
    padding: Int,
    endOfStream: Boolean
  ): Int = {
    val length = data.readableBytes
    val stream = connection.stream(streamId)
    val headers = stream.getProperty(headersKey).asInstanceOf[Http2Headers]

    // we retain this because the ref count it comes in with is
    // actually a residual one from ByteToMessageDecoder, and will be
    // decremented later by ByteToMessageDecoder when we return
    // control up the stack to the decoder.  This means that if we
    // want to use this message after, we need to retain it ourselves.
    data.retain()
    val msg = if (headers == null) {
      if (endOfStream) new DefaultLastHttpContent(data) else new DefaultHttpContent(data)
    } else {
      if (endOfStream) {
        val rep = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          HttpConversionUtil.parseStatus(headers.status),
          data,
          false /* validateHeaders */
        )
        HttpConversionUtil.addHttp2ToHttpHeaders(
          streamId,
          headers,
          rep,
          false /* validateHeaders */
        )
        stream.removeProperty(headersKey)
        rep
      } else {
        val rep = new DefaultHttpResponse(
          HttpVersion.HTTP_1_1,
          HttpConversionUtil.parseStatus(headers.status),
          false /* validateHeaders */
        )
        HttpConversionUtil.addHttp2ToHttpHeaders(
          streamId,
          headers,
          rep.headers,
          HttpVersion.HTTP_1_1,
          false /* isTrailer */,
          false /* isRequest */
        )
        HttpUtil.setTransferEncodingChunked(rep, true)
        ctx.fireChannelRead(Message(rep, streamId))
        stream.removeProperty(headersKey)
        new DefaultHttpContent(data)
      }
    }
    ctx.fireChannelRead(Message(msg, streamId))
    length + padding
    // returning this means that we've already processed all of the bytes, and
    // tells the http/2 backpressure implementation that it's OK to accept
    // more bytes.  the disadvantage of this is that it doesn't take the work
    // of the application into account, but the advantage is that it's far
    // simpler to implement.
  }

  override def onHeadersRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    newHeaders: Http2Headers,
    padding: Int,
    endOfStream: Boolean
  ): Unit = {
    val stream = connection.stream(streamId)
    val prop = stream.getProperty(headersKey).asInstanceOf[Http2Headers]
    val headers = if (prop == null) new DefaultHttp2Headers() else prop
    headers.add(newHeaders)

    if (endOfStream) {
      val req = HttpConversionUtil.toHttpResponse(
        streamId,
        headers,
        ctx.alloc(),
        false /* validateHttpHeaders */
      )
      ctx.fireChannelRead(Message(req, streamId))
      stream.removeProperty(headersKey)
    } else {
      stream.setProperty(headersKey, headers)
    }
  }

  // this one is only called on END_HEADERS, so it's safe to decide as an HttpResponse
  // or to collate into a FullHttpResponse.
  override def onHeadersRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    newHeaders: Http2Headers,
    streamDependency: Int,
    weight: Short,
    exclusive: Boolean,
    padding: Int,
    endOfStream: Boolean
  ): Unit = {
    onHeadersRead(ctx, streamId, newHeaders, padding, endOfStream)
    val stream = connection.stream(streamId)
    val headers = stream.getProperty(headersKey).asInstanceOf[Http2Headers]
    if (headers != null && !headers.contains(HttpHeaderNames.CONTENT_LENGTH)) {
      val rep = new DefaultHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpConversionUtil.parseStatus(headers.status),
        false /* validateHeaders */
      )
      HttpConversionUtil.addHttp2ToHttpHeaders(
        streamId,
        headers,
        rep.headers,
        HttpVersion.HTTP_1_1,
        false /* isTrailer */,
        false /* isRequest */
      )
      HttpUtil.setTransferEncodingChunked(rep, true)
      ctx.fireChannelRead(Message(rep, streamId))
      stream.removeProperty(headersKey)
    }
  }

  override def onRstStreamRead(ctx: ChannelHandlerContext, streamId: Int, errorCode: Long): Unit = {
    ctx.fireChannelRead(Rst(streamId, errorCode))
  }

  override def onGoAwayRead(
    ctx: ChannelHandlerContext,
    lastStreamId: Int,
    errorCode: Long,
    debug: ByteBuf
  ): Unit = {

    // TODO: this is very ad-hoc right now, we need to come up with a consistent way of
    // downconverting.
    val idx = debug.readerIndex
    val sliced = debug.slice(
      idx,
      math.min(idx + HeaderTooLargeBytes.readableBytes, idx + debug.readableBytes)
    )
    val status = if (sliced == HeaderTooLargeBytes) {
      HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE
    } else HttpResponseStatus.BAD_REQUEST

    val rep = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status)
    ctx.fireChannelRead(GoAway(rep))
  }
}

private[http2] object Http2ClientDowngrader {

  // this is a magic string from the netty server implementation.  it's the debug
  // data it includes in the GOAWAY when the headers are too long.
  val HeaderTooLargeBytes =
    Unpooled.copiedBuffer("Header size exceeded max allowed bytes", UTF_8)

  sealed trait StreamMessage
  case class Message(obj: HttpObject, streamId: Int) extends StreamMessage
  case class GoAway(obj: HttpObject) extends StreamMessage
  case class Rst(streamId: Int, errorCode: Long) extends StreamMessage
}
