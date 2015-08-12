package com.twitter.finagle.httpx.util

import com.twitter.finagle.httpx.Request
import org.jboss.netty.handler.codec.http.multipart.{Attribute, HttpPostRequestDecoder}
import scala.collection.JavaConverters._

private[twitter] object RequestDecoder {
  /**
   * Helper for decoding request bodies. This decoder only handles attributes
   * and will fail on chunked requests.
   *
   * Note: Potentially mutates the request.
   */
  def decode(request: Request): Map[String, String] = {
    require(!request.isChunked)
    val decoder = new HttpPostRequestDecoder(request.httpRequest)
    decoder.getBodyHttpDatas.asScala.flatMap {
      case attr: Attribute => Some(attr.getName -> attr.getValue)
      case _ => None
    }.toMap
  }
}
