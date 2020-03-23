package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.exp.{Multipart, MultipartDecoder}
import com.twitter.finagle.http.Request
import com.twitter.io.Buf
import com.twitter.util.StorageUnit
import io.netty.handler.codec.http.multipart._
import scala.collection.mutable
import scala.jdk.CollectionConverters._

private[finagle] class Netty4MultipartDecoder extends MultipartDecoder {
  protected def decodeFull(req: Request, maxInMemoryFileSize: StorageUnit): Option[Multipart] = {
    val decoder = new HttpPostMultipartRequestDecoder(
      new DefaultHttpDataFactory(maxInMemoryFileSize.inBytes),
      Bijections.finagle.requestToNetty(req, req.contentLength)
    )

    val attrs = new mutable.HashMap[String, mutable.ListBuffer[String]]()
    val files = new mutable.HashMap[String, mutable.ListBuffer[Multipart.FileUpload]]()

    decoder.getBodyHttpDatas.asScala.foreach {
      case attr: Attribute =>
        val buf = attrs.getOrElseUpdate(attr.getName, mutable.ListBuffer[String]())
        buf += attr.getValue

      case fu: FileUpload =>
        val buf = files
          .getOrElseUpdate(fu.getName, mutable.ListBuffer[Multipart.FileUpload]())
        if (fu.isInMemory) {
          buf += Multipart.InMemoryFileUpload(
            Buf.ByteArray.Owned(fu.get()),
            fu.getContentType,
            fu.getFilename,
            fu.getContentTransferEncoding
          )
        } else {
          buf += Multipart.OnDiskFileUpload(
            fu.getFile,
            fu.getContentType,
            fu.getFilename,
            fu.getContentTransferEncoding
          )
        }

      case _ => // ignore everything else
    }

    Some(Multipart(attrs.mapValues(_.toSeq).toMap, files.mapValues(_.toSeq).toMap))
  }
}
