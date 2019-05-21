package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.netty4.http.FinagleHttpObjectAggregator
import com.twitter.util.StorageUnit
import io.netty.handler.codec.http._

/**
 * Aggregates fixed length http messages and emits [[FullHttpMessage]] downstream.
 *
 * Http message is considered fixed length if it has `Content-Length` header and
 * `Transfer-Encoding` header is not `chunked` or if we can deduce actual content
 * length to be 0 even if `Content-Length` header is not specified.
 *
 * Fixed length messages may arrive as a series of chunks (not to be confused with chunks
 * as in `Transfer-Encoding: chunked`) from the upstream handlers in pipeline. To
 * eventually build a [[com.twitter.finagle.http.Request]] object with content available
 * as [[com.twitter.finagle.http.Request.content]] or
 * [[com.twitter.finagle.http.Request.contentString]] all chunks must be stored in a
 * temporary buffer and combined together into a [[FullHttpMessage]] as soon as the last
 * chunk of the message is received.
 *
 * The `maxContentLength` determines when to aggregate chunks and when to bypass the
 * message as is. Only sufficiently small messages (smaller than `maxContentLength`) are
 * aggregated.
 *
 * Messages with `Transfer-Encoding: chunked` are always bypassed.
 */
private[http] class FixedLengthMessageAggregator(
  maxContentLength: StorageUnit,
  handleExpectContinue: Boolean = true)
    extends FinagleHttpObjectAggregator(maxContentLength.inBytes.toInt, handleExpectContinue) {
  require(maxContentLength.bytes >= 0)

  override def isStartMessage(msg: HttpObject): Boolean = msg match {
    case httpMsg: HttpMessage => shouldAggregate(httpMsg)
    case _ => false
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
