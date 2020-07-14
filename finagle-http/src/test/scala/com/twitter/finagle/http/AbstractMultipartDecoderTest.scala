package com.twitter.finagle.http

import com.twitter.finagle.http.exp.{Multipart, MultipartDecoder}
import com.twitter.io.{Buf, BufReader, Files}
import com.twitter.util.{Await, Duration}
import org.scalatest.FunSuite
import com.twitter.io.Reader

abstract class AbstractMultipartDecoderTest(decoder: MultipartDecoder) extends FunSuite {
  /*
   * The generated request is equivalent to the following form:
   *
   * <form enctype="multipart/form-data" action="/groups_file?debug=true" method="POST">
   *   <label for="groups">Filename:</label>
   *   <input type="file" name="groups" id="groups"><br>
   *   <input type="hidden" name="type" value="text"/>
   *   <input type="submit" name="submit" value="Submit">
   * </form>
   */
  private[this] def newRequest(buf: Buf): Request =
    RequestBuilder()
      .url("http://example.com")
      .add(FileElement("groups", buf, Some("image/gif"), Some("dealwithit.gif")))
      .add(SimpleElement("type", "text"))
      .buildFormPost(multipart = true)

  private[this] def newChunkedRequest(buf: Buf): Request = {
    val req = newRequest(buf)
    val chunkedReq = new Request.Inbound(
      Reader.concat(Seq(BufReader(req.content, 2).map(Chunk(_)), Reader.value(Chunk.lastEmpty))),
      req.remoteSocketAddress,
      req.trailers
    )
    chunkedReq.method = req.method
    chunkedReq.uri = req.uri
    req.headerMap.filter(_._1 != Fields.ContentLength).map(chunkedReq.headerMap += )
    chunkedReq.headerMap += Fields.TransferEncoding -> "chunked"
    chunkedReq.setChunked(true)
    chunkedReq
  }

  test("Attribute") {
    assert(decode(newRequest(Buf.Empty)).get.attributes("type").head == "text")
  }

  test("FileUpload (in-memory)") {
    val foo = Buf.Utf8("foo")
    val multipart = decode(newRequest(foo)).get

    val Multipart.InMemoryFileUpload(buf, contentType, fileName, contentTransferEncoding) =
      multipart.files("groups").head
    val attr = multipart.attributes("type").head

    assert(buf == foo)
    assert(contentType == "image/gif")
    assert(fileName == "dealwithit.gif")
    assert(contentTransferEncoding == "binary")
    assert(attr == "text")
  }

  test("FileUpload (on-disk)") {
    val foo = Buf.Utf8("." * (Multipart.DefaultMaxInMemoryFileSize.inBytes.toInt + 10))
    val multipart = decode(newRequest(foo)).get

    val Multipart.OnDiskFileUpload(file, contentType, fileName, contentTransferEncoding) =
      multipart.files("groups").head
    val attr = multipart.attributes("type").head

    assert(Buf.ByteArray.Owned(Files.readBytes(file, limit = Int.MaxValue)) == foo)
    assert(contentType == "image/gif")
    assert(fileName == "dealwithit.gif")
    assert(contentTransferEncoding == "binary")
    assert(attr == "text")
  }

  test("FileUpload (chunked, in-memory)") {
    val foo = Buf.Utf8("foo")
    val multipart = decode(newChunkedRequest(foo)).get

    val Multipart.InMemoryFileUpload(buf, contentType, fileName, contentTransferEncoding) =
      multipart.files("groups").head
    val attr = multipart.attributes("type").head

    assert(buf == foo)
    assert(contentType == "image/gif")
    assert(fileName == "dealwithit.gif")
    assert(contentTransferEncoding == "binary")
    assert(attr == "text")
  }

  test("FileUpload (chunked, on-disk)") {
    val foo = Buf.Utf8("." * (Multipart.DefaultMaxInMemoryFileSize.inBytes.toInt + 10))
    val multipart = decode(newChunkedRequest(foo)).get

    val Multipart.OnDiskFileUpload(file, contentType, fileName, contentTransferEncoding) =
      multipart.files("groups").head
    val attr = multipart.attributes("type").head

    assert(Buf.ByteArray.Owned(Files.readBytes(file, limit = Int.MaxValue)) == foo)
    assert(contentType == "image/gif")
    assert(fileName == "dealwithit.gif")
    assert(contentTransferEncoding == "binary")
    assert(attr == "text")
  }

  test("Not a multipart request") {
    assert(decode(Request()).isEmpty)
    assert(decode(Request(Method.Post, "/")).isEmpty)
    assert(
      decode(
        { val r = Request(Method.Post, "/"); r.contentType = "application/json"; r }
      ).isEmpty
    )
    assert(
      decode(
        { val r = Request(Method.Put, "/"); r.contentType = "multipart/form-data"; r }
      ).isEmpty
    )
  }

  private def decode(r: Request) =
  Await.result(decoder.decode(r), Duration.fromSeconds(5))
}
