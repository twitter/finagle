package com.twitter.finagle.http2.transport

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.{HttpHeaders, HttpObject, HttpResponse}
import io.netty.handler.codec.http2.Http2ServerDowngrader
import java.util.{List => JList}
import scala.collection.JavaConverters._

private[http2] object RichHttp2ServerDowngrader {
  def stripConnectionHeaders(headers: HttpHeaders): Unit = {
    // Look at the values of the connection header to find additional headers
    // that need to be removed.
    headers.getAll("connection").asScala.foreach(headers.remove(_))

    // Clobber all values of "te" except for "trailers"
    if (headers.containsValue("te", "trailers", true /* ignoreCase */))
      headers.set("te", "trailers")
    else
      headers.remove("te")
  }
}

/**
 * Netty uses a blacklist containing common connection-related headers that
 * should be removed from h2 messages. However, two cases need to be handled
 * separately:
 *   1) All headers mentioned in the "connection" header must be stripped
 *   2) The "TE" header can exist iff its value is "trailers"
 * See https://tools.ietf.org/html/rfc7540#section-8.1.2.2
 */
private[http2] class RichHttp2ServerDowngrader(
    validateHeaders: Boolean
  ) extends Http2ServerDowngrader(validateHeaders) {
  import RichHttp2ServerDowngrader._

  override def encode(ctx: ChannelHandlerContext, obj: HttpObject, out: JList[Object]): Unit = {
    obj match {
      case res: HttpResponse => stripConnectionHeaders(res.headers)
      case _ => // nop
    }

    super.encode(ctx, obj, out)
  }
}
