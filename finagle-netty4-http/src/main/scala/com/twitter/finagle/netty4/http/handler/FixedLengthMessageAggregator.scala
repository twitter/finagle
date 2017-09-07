package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.netty4.http.FinagleHttpObjectAggregator
import com.twitter.util.StorageUnit
import io.netty.handler.codec.http._

/**
 * netty can chunk fixed length messages so we dechunk them if the content-length
 * is sufficiently small so that streaming clients and servers can access message
 * contents via `content` and `contentString`. Chunked transfer encoded messages
 * still require accessing `reader`.
 */
private[http] class FixedLengthMessageAggregator(
  maxContentLength: StorageUnit,
  handleExpectContinue: Boolean = true
) extends FinagleHttpObjectAggregator(maxContentLength.inBytes.toInt, handleExpectContinue) {
  require(maxContentLength.bytes >= 0)

  private[this] var decoding = false

  override def acceptInboundMessage(msg: Any): Boolean = msg match {
    case _: FullHttpMessage =>
      false

    case msg: HttpMessage if shouldAggregate(msg) =>
      decoding = true
      true

    case _: HttpContent if decoding =>
      true

    case _ =>
      false
  }

  override def finishAggregation(aggregated: FullHttpMessage): Unit = {
    decoding = false
    super.finishAggregation(aggregated)
  }

  private[this] def shouldAggregate(msg: HttpMessage): Boolean = {
    // We never dechunk 'Transfer-Encoding: chunked' messages
    if (HttpUtil.isTransferEncodingChunked(msg)) false
    else if (noContentResponse(msg)) true // No body so aggregate the LastHttpContent
    else {
      // We will dechunk a message if it has a content-length header that is less
      // than or equal to the maxContentLength parameter.
      val contentLength = HttpUtil.getContentLength(msg, -1L)

      if (contentLength != -1L) contentLength <= maxContentLength.bytes
      else { // No content-length header.

        // Requests without a transfer-encoding or content-length header cannot have a body
        // (see https://tools.ietf.org/html/rfc7230#section-3.3.3). Netty 4 will signal
        // end-of-message for these requests with an immediate follow up LastHttpContent, so
        // we aggregate it as to not end up with a chunked request and a Reader that will
        // only signal EOF that folks are probably not handling anyway.
        msg.isInstanceOf[HttpRequest]
      }
    }
  }

  private[this] def noContentResponse(msg: HttpMessage): Boolean = msg match {
    case res: HttpResponse =>
      res.status.code match {
        case 101 =>
          // The Hixie 76 websocket handshake response may have content
          !((res.headers.contains(HttpHeaderNames.SEC_WEBSOCKET_ACCEPT)) &&
            (res.headers.contains(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true)))

        case code if code >= 100 && code < 200 => true
        case 204 | 205 | 304 => true
        case _ => false
      }

    case _ => false
  }
}
