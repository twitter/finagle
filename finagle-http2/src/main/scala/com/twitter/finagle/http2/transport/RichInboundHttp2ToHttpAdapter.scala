package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.Http2Transporter
import com.twitter.io.Charsets
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.{HttpResponseStatus, HttpVersion, DefaultFullHttpResponse}
import io.netty.handler.codec.http2.{Http2Connection, InboundHttp2ToHttpAdapter}

/**
 * An `InboundHttp2ToHttpAdapter` that encodes GOAWAYS as `HttpResponses` with a
 * stream id of -1.
 */
private[http2] class RichInboundHttp2ToHttpAdapter(
    connection: Http2Connection,
    maxContentLength: Int)
  extends InboundHttp2ToHttpAdapter(
    connection,
    maxContentLength,
    false /* validateHttpHeaders */,
    false /* propagateSettings */) {

  override def onGoAwayRead(
    ctx: ChannelHandlerContext,
    lastStreamId: Int,
    errorCode: Long,
    debugData: ByteBuf
  ): Unit = {

    val debugString = debugData.toString(Charsets.Utf8)

    // TODO: this is very ad-hoc right now, we need to come up with a consistent way of
    // downconverting.
    val status = if (debugString.startsWith("Header size exceeded max allowed bytes")) {
      HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE
    } else HttpResponseStatus.BAD_REQUEST

    val rep = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status)

    // we set to -1 to signal it's a GOAWAY
    Http2Transporter.setStreamId(rep, -1)
    ctx.fireChannelRead(rep)
  }
}
