package com.twitter.finagle.http.exp

import com.twitter.finagle.http.{MediaType, Method, Request}


/**
 * An internal utility that represents a decoder for a [[Multipart]] data.
 *
 * This decoder is used from within `Request.multipart` hence the constraint on HTTP method
 * being POST.
 */
private[finagle] abstract class MultipartDecoder {

  protected def decode(request: Request): Option[Multipart]

  final def apply(request: Request): Option[Multipart] =
    if (request.isChunked || !MultipartDecoder.isMultipart(request)) None
    else decode(request)
}

private[finagle] object MultipartDecoder {

  private val contentTypeIsMultipart: String => Boolean =
    _.startsWith(MediaType.MultipartForm)

  private def isMultipart(request: Request): Boolean =
    request.method == Method.Post && request.contentType.exists(contentTypeIsMultipart)

  val Empty: MultipartDecoder = new MultipartDecoder {
    def decode(request: Request): Option[Multipart] = None
  }
}
