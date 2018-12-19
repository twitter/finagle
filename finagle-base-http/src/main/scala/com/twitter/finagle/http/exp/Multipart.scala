package com.twitter.finagle.http.exp

import com.twitter.conversions.StorageUnitOps._
import com.twitter.io.Buf
import com.twitter.util.StorageUnit
import java.io.File

/**
 * Provides a convenient interface for querying the content of the `multipart/form-data` body.
 *
 * To decode the non-chunked, POST request into an instance of [[Multipart]], use `Request.multipart`.
 *
 * Note: This is an _experimental API_, which will likely be changed in future to support
 * streaming HTTP requests.
 **/
case class Multipart(
  attributes: Map[String, Seq[String]],
  files: Map[String, Seq[Multipart.FileUpload]])

/**
 * A set of utility classes and methods for decoding HTTP POST requests with
 * `multipart/form-data` content type.
 */
object Multipart {

  /**
   * The maximum size of the file upload to be stored as in-memory file.
   */
  val DefaultMaxInMemoryFileSize: StorageUnit = 32.kilobytes

  /**
   * A type representing a multipart _file upload_.
   */
  trait FileUpload {

    /**
     * The Content-Type of this file upload.
     */
    def contentType: String

    /**
     * An original filename in the client's filesystem, as provided by the browser (or other
     * client software).
     */
    def fileName: String

    /**
     * The Content-Transfer-Encoding of this file upload.
     */
    def contentTransferEncoding: String
  }

  /**
   * A variant of [[FileUpload]] that is already in memory and represented as [[Buf]].
   */
  final case class InMemoryFileUpload(
    content: Buf,
    contentType: String,
    fileName: String,
    contentTransferEncoding: String)
      extends FileUpload

  /**
   * A variant of [[FileUpload]] that is stored on disk and represented as [[File]].
   */
  final case class OnDiskFileUpload(
    content: File,
    contentType: String,
    fileName: String,
    contentTransferEncoding: String)
      extends FileUpload
}
