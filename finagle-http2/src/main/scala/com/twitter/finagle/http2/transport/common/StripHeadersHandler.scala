package com.twitter.finagle.http2.transport.common

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http.{HttpHeaderNames, HttpHeaders, HttpMessage}
import io.netty.handler.codec.http2.Http2CodecUtil

/**
 * Netty uses a denylist containing common connection-related headers that
 * should be removed from h2 messages. However, a few cases need to be handled
 * separately:
 *   1) All headers mentioned in the "connection" header must be stripped
 *   2) The "TE" header can exist iff its value is "trailers"
 * See https://tools.ietf.org/html/rfc7540#section-8.1.2.2
 *   3) The "http2-settings" headers is left on the server side, but we probably
 * don't need it anymore.
 */
@Sharable
object StripHeadersHandler extends ChannelDuplexHandler {
  val HandlerName = "stripHeaders"

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    stripHeaders(msg)
    super.channelRead(ctx, msg)
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, p: ChannelPromise): Unit = {
    stripHeaders(msg)
    super.write(ctx, msg, p)
  }

  private[this] def stripHeaders(msg: Object): Unit = msg match {
    case msg: HttpMessage => stripConnectionHeaders(msg.headers)
    case _ => // nop
  }

  private[this] def stripConnectionHeaders(headers: HttpHeaders): Unit = {
    // Look at the values of the connection header to find additional headers
    // that need to be removed.
    val connectionHeaders = headers.getAll(HttpHeaderNames.CONNECTION)
    if (!connectionHeaders.isEmpty) {
      val iter = connectionHeaders.iterator()
      while (iter.hasNext()) {
        headers.remove(iter.next())
      }
      headers.remove(HttpHeaderNames.CONNECTION)
    }

    // Clobber all values of "te" (short for transfer-encoding) except for "trailers"
    // https://tools.ietf.org/html/rfc7540#section-8.1.2.2
    if (headers.containsValue(
        HttpHeaderNames.TE,
        HttpHeaders.Values.TRAILERS,
        true /* ignoreCase */
      ))
      headers.set(HttpHeaderNames.TE, HttpHeaders.Values.TRAILERS)
    else
      headers.remove(HttpHeaderNames.TE)

    // "http2-settings" is a special header that the h2c upgrade uses to pass settings
    // without having to wait for the upgrade to finish.
    headers.remove(Http2CodecUtil.HTTP_UPGRADE_SETTINGS_HEADER)

    // "upgrade" is a special header that the h2c upgrade uses to signal that it wants
    // to add to h2c
    headers.remove(HttpHeaderNames.UPGRADE)
  }
}
