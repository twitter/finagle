package com.twitter.finagle.http.exp

import com.twitter.finagle.http.{MediaType, Method, Request}
import com.twitter.finagle.util.LoadService
import com.twitter.util.StorageUnit

/**
 * A utility that represents a decoder for a [[Multipart]] data.
 *
 * This decoder is used from within `Request.multipart` hence the constraint on HTTP method
 * being POST.
 */
abstract class MultipartDecoder {

  protected def decodeFull(request: Request, maxInMemoryFileSize: StorageUnit): Option[Multipart]

  final def decode(request: Request): Option[Multipart] =
    decode(request, Multipart.DefaultMaxInMemoryFileSize)

  final def decode(request: Request, maxInMemoryFileSize: StorageUnit): Option[Multipart] =
    if (request.isChunked || !MultipartDecoder.isMultipart(request)) None
    else decodeFull(request, maxInMemoryFileSize)
}

object MultipartDecoder extends MultipartDecoder {

  val Empty: MultipartDecoder = new MultipartDecoder {
    def decodeFull(request: Request, maxInMemoryFileSize: StorageUnit): Option[Multipart] = None
  }

  // A service-loaded `MultipartDecoder`.
  private val underlying: MultipartDecoder =
    LoadService[MultipartDecoder]().headOption.getOrElse(Empty)

  private val contentTypeIsMultipart: String => Boolean =
    _.startsWith(MediaType.MultipartForm)

  private def isMultipart(request: Request): Boolean =
    request.method == Method.Post && request.contentType.exists(contentTypeIsMultipart)

  protected def decodeFull(request: Request, maxInMemoryFileSize: StorageUnit): Option[Multipart] =
    underlying.decode(request)
}
