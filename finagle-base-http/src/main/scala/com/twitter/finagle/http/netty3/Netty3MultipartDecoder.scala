package com.twitter.finagle.http.netty3

import com.twitter.finagle.http.exp.{Multipart, MultipartDecoder}
import com.twitter.finagle.http.Request
import com.twitter.io.Buf
import org.jboss.netty.handler.codec.http.multipart._
import scala.collection.JavaConverters._
import scala.collection.mutable

private[finagle] object Netty3MultipartDecoder extends MultipartDecoder {
  protected def decode(req: Request): Option[Multipart] = {
    val decoder = new HttpPostMultipartRequestDecoder(
      new DefaultHttpDataFactory(Multipart.MaxInMemoryFileSize.inBytes),
      Bijections.requestToNetty(req)
    )

    val attrs = new mutable.HashMap[String, mutable.ListBuffer[String]]()
    val files = new mutable.HashMap[String, mutable.ListBuffer[Multipart.FileUpload]]()

    decoder.getBodyHttpDatas.asScala.foreach {
      case attr: Attribute =>
        val buf = attrs.getOrElseUpdate(attr.getName, mutable.ListBuffer[String]())
        buf += attr.getValue

      case fu: FileUpload =>
        val buf = files.getOrElseUpdate(fu.getName, mutable.ListBuffer[Multipart.FileUpload]())
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
