package com.twitter.finagle.http.exp

import com.twitter.conversions.storage._
import com.twitter.finagle.http.{Method, Request}
import com.twitter.io.Buf
import com.twitter.util.StorageUnit
import org.jboss.netty.handler.codec.http.multipart
import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Provides a convenient interface for querying the content of the `multipart/form-data` body.
 *
 * To decode the non-chunked, POST request into an instance of [[Multipart]], use [[Request.multipart]].
 *
 * Note: This is an _experimental API_, which will likely be changed in future to support
 * streaming HTTP requests.
 **/
case class Multipart(
    attributes: Map[String, Seq[String]],
    files: Map[String, Seq[Multipart.FileUpload]])

/**
 * An _experimental_ set ot utility classes and methods for decoding HTTP POST requests with
 * `multipart/form-data` content type.
 */
object Multipart {

  /**
   * The maximum size of the file upload to be stored as in-memory file.
   */
  private[http] val MaxInMemoryFileSize: StorageUnit = 32.kilobytes

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
      contentTransferEncoding: String) extends FileUpload

  /**
   * A variant of [[FileUpload]] that is stored on disk and represented as [[File]].
   */
  final case class OnDiskFileUpload(
      content: File,
      contentType: String,
      fileName: String,
      contentTransferEncoding: String) extends FileUpload

  /**
   * Decodes the _non-chunked_ (a complete) HTTP `request` with `multipart/form-data`
   * content type into an instance of [[Multipart]].
   *
   * **Note** that this method _mutates_ the given `request`. So it's not possible
   * read the [[Multipart]] data from the same request twice.
   *
   * See [[https://groups.google.com/forum/#!topic/netty/NxT-4QzutI4 this Netty thread]]
   * for more details.
   */
  private[http] def decodeNonChunked(request: Request): Multipart = {
    require(!request.isChunked)
    require(request.method == Method.Post)

    val decoder = new multipart.HttpPostRequestDecoder(
      new multipart.DefaultHttpDataFactory(MaxInMemoryFileSize.inBytes), request.httpRequest)
    val attrs = new mutable.HashMap[String, mutable.ListBuffer[String]]()
    val files = new mutable.HashMap[String, mutable.ListBuffer[FileUpload]]()

    decoder.getBodyHttpDatas.asScala.foreach {
      case attr: multipart.Attribute =>
        val buf = attrs.getOrElseUpdate(attr.getName, mutable.ListBuffer[String]())
        buf += attr.getValue

      case fu: multipart.FileUpload =>
        val buf = files.getOrElseUpdate(fu.getName, mutable.ListBuffer[FileUpload]())
        if (fu.isInMemory) {
          buf += InMemoryFileUpload(
            Buf.ByteArray.Owned(fu.get()),
            fu.getContentType,
            fu.getFilename,
            fu.getContentTransferEncoding
          )
        } else {
          buf += OnDiskFileUpload(
            fu.getFile,
            fu.getContentType,
            fu.getFilename,
            fu.getContentTransferEncoding
          )
        }

      case _ => // ignore everything else
    }

    Multipart(attrs.mapValues(_.toSeq).toMap, files.mapValues(_.toSeq).toMap)
  }
}