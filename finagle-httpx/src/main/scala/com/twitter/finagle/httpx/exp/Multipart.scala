package com.twitter.finagle.httpx.exp

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.jboss.netty.handler.codec.http.multipart

import com.twitter.finagle.httpx.Request
import com.twitter.io.Buf

/**
 * Provides a convenient interface for querying the content of the `multipart/form-data` body.
 *
 * Use `Multipart.decodeNonChunked` to turn a non-chunked HTTP request into an instance of
 * the [[Multipart]] class.
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
   * A data type representing a multipart _file upload_.
   */
  case class FileUpload(
      content: Buf,
      contentType: String,
      fileName: String,
      contentTransferEncoding: String)

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
  def decodeNonChunked(request: Request): Multipart = {
    require(!request.isChunked)

    val decoder = new multipart.HttpPostRequestDecoder(request.httpRequest)
    val attrs = new mutable.HashMap[String, mutable.ListBuffer[String]]()
    val files = new mutable.HashMap[String, mutable.ListBuffer[FileUpload]]()

    decoder.getBodyHttpDatas.asScala.foreach {
      case attr: multipart.Attribute =>
        val buf = attrs.getOrElseUpdate(attr.getName, mutable.ListBuffer[String]())
        buf += attr.getValue

      case fu: multipart.FileUpload =>
        val buf = files.getOrElseUpdate(fu.getName, mutable.ListBuffer[FileUpload]())
        buf += FileUpload(
          Buf.ByteArray.Owned(fu.get()),
          fu.getContentType,
          fu.getFilename,
          fu.getContentTransferEncoding
        )

      case _ => // ignore everything else
    }

    Multipart(attrs.mapValues(_.toSeq).toMap, files.mapValues(_.toSeq).toMap)
  }
}