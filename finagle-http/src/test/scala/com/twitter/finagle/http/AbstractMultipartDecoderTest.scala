package com.twitter.finagle.http

import com.twitter.finagle.http.exp.{Multipart, MultipartDecoder}
import com.twitter.io.{Buf, Files}
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractMultipartDecoderTest(decoder: MultipartDecoder) extends AnyFunSuite {

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

  test("Attribute") {
    assert(decoder.decode(newRequest(Buf.Empty)).get.attributes("type").head == "text")
  }

  test("FileUpload (in-memory)") {
    val foo = Buf.Utf8("foo")
    val multipart = decoder.decode(newRequest(foo)).get

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
    val multipart = decoder.decode(newRequest(foo)).get

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
    assert(decoder.decode(Request()).isEmpty)
    assert(decoder.decode(Request(Method.Post, "/")).isEmpty)
    assert(
      decoder
        .decode(
          { val r = Request(Method.Post, "/"); r.contentType = "application/json"; r }
        ).isEmpty
    )
    assert(
      decoder
        .decode(
          { val r = Request(Method.Put, "/"); r.contentType = "multipart/form-data"; r }
        ).isEmpty
    )
  }
}
