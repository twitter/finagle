package com.twitter.finagle.http.exp

import com.twitter.finagle.http.Request

/**
 * An internal utility that represents a decoder for a [[Multipart]] data.
 */
private[finagle] trait MultipartDecoder {
  def apply(request: Request): Option[Multipart]
}

private[finagle] object MultipartDecoder {
  val Empty: MultipartDecoder = new MultipartDecoder {
    def apply(request: Request): Option[Multipart] = None
  }
}
