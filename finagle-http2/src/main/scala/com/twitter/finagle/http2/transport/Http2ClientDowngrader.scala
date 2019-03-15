package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.transport.StreamMessage._
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2.{
  Http2Connection,
  Http2EventAdapter,
  Http2Headers,
  HttpConversionUtil
}
import java.nio.charset.StandardCharsets.UTF_8

/**
 * `Http2ClientDowngrader` wraps RSTs, GOAWAYs, HEADERS, Pings, and DATA in thin
 * finagle wrappers.
 */
private[http2] final class Http2ClientDowngrader(connection: Http2Connection)
    extends Http2EventAdapter {

  // this is a magic string from the netty server implementation.  it's the debug
  // data it includes in the GOAWAY when the headers are too long.
  private val HeaderTooLargeBytes =
    Unpooled.copiedBuffer("Header size exceeded max allowed bytes", UTF_8)

  // Http2EventAdapter overrides

  override def onDataRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    data: ByteBuf,
    padding: Int,
    endOfStream: Boolean
  ): Int = {
    val length = data.readableBytes

    // we retain this because the ref count it comes in with is
    // actually a residual one from ByteToMessageDecoder, and will be
    // decremented later by ByteToMessageDecoder when we return
    // control up the stack to the decoder. This means that if we
    // want to use this message after, we need to retain it ourselves.
    data.retain()
    val msg = if (endOfStream) new DefaultLastHttpContent(data) else new DefaultHttpContent(data)

    ctx.fireChannelRead(Message(msg, streamId))
    length + padding
    // returning this means that we've already processed all of the bytes, and
    // tells the http/2 backpressure implementation that it's OK to accept
    // more bytes.  the disadvantage of this is that it doesn't take the work
    // of the application into account, but the advantage is that it's far
    // simpler to implement.
  }

  // Called when a full HEADERS sequence has been received that does not contain priority info.
  override def onHeadersRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    headers: Http2Headers,
    padding: Int,
    endOfStream: Boolean
  ): Unit = {
    if (HttpResponseStatus.CONTINUE.codeAsText.contentEquals(headers.status)) {
      // 100-continue response is a special case where Http2HeadersFrame#isEndStream=false
      // but we need to decode it as a FullHttpResponse to play nice with HttpObjectAggregator.
      // (this workaround can go away once we move to the netty multipex client)
      val msg = HttpConversionUtil.toFullHttpResponse(
        streamId,
        headers,
        ctx.alloc(),
        false /* validateHttpHeaders */
      )

      ctx.fireChannelRead(Message(msg, streamId))
    } else if (endOfStream) {
      // These are the last headers in the stream. They could either be initial or trailing. We can
      // short-circuit to a FullHttpMessage if these are the initial headers.
      if (connection.stream(streamId).isTrailersReceived) {
        val msg = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER, /*validateHeaders*/ false)

        HttpConversionUtil.addHttp2ToHttpHeaders(
          streamId,
          headers,
          msg.trailingHeaders,
          HttpVersion.HTTP_1_1,
          /*isTrailer*/ true,
          /*isRequest*/ false
        )

        ctx.fireChannelRead(Message(msg, streamId))
      } else {
        val msg = HttpConversionUtil.toFullHttpResponse(
          streamId,
          headers,
          ctx.alloc(),
          false /* validateHttpHeaders */
        )

        ctx.fireChannelRead(Message(msg, streamId))
      }
    } else {
      // These are the initial headers that don't terminate the stream. We're converting them to a
      // regular HttpResponse.
      //
      // Unfortunately Netty doesn't have tools for converting to a non-full
      // HttpResponse so we just do it ourselves: it's not that hard anyway.
      val status = HttpConversionUtil.parseStatus(headers.status)
      val msg = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status, /*validateHeaders*/ false)
      HttpConversionUtil.addHttp2ToHttpHeaders(
        streamId,
        headers,
        msg.headers,
        HttpVersion.HTTP_1_1,
        /*isTrailer*/ false,
        /*isRequest*/ false
      )

      ctx.fireChannelRead(Message(msg, streamId))
    }
  }

  // Called when a full HEADERS sequence has been received that does contain priority info.
  // Since we don't care about priority info, we just ignore the priority info and delegate
  // to the other `onHeadersRead` method.
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
    ctx.fireChannelRead(GoAway(rep, lastStreamId, errorCode))
  }

  override def onPingAckRead(ctx: ChannelHandlerContext, data: Long): Unit = {
    ctx.fireChannelRead(Ping)
  }
}
