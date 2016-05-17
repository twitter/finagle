package com.twitter.finagle.netty4.http.handler

import com.twitter.util.StorageUnit
import io.netty.handler.codec.http._

/**
 * netty can chunk fixed length messages so we dechunk them so that streaming clients
 * and servers can access message contents via `content` and `contentString`. Chunked
 * transfer encoded messages still require accessing `reader`.
 */
class FixedLengthMessageAggregator(maxContentLength: StorageUnit)
  extends HttpObjectAggregator(maxContentLength.inBytes.toInt) {

  var decoding = false


  override def acceptInboundMessage(msg: Any): Boolean = msg match {
    case _: FullHttpMessage =>
      false

    case msg: HttpMessage if !HttpUtil.isTransferEncodingChunked(msg) =>
      decoding = true
      true

    case content: HttpContent if decoding =>
      true

    case _ =>
      false
  }


  override def finishAggregation(aggregated: FullHttpMessage): Unit = {
    decoding = false
    super.finishAggregation(aggregated)
  }
}
