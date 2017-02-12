package com.twitter.finagle.http.exp

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.twitter.finagle.http.{FileElement, Request, RequestBuilder, SimpleElement, Method}
import com.twitter.io.{Files, Buf}

@RunWith(classOf[JUnitRunner])
class MultipartTest extends FunSuite {

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

  test("sanity check") {
    val req = Request()
    req.method = Method.Post
    req.contentString = "abc=foo&def=123&abc=bar"

    assert(req.multipart == Some(Multipart(Map(
      "abc" -> Seq("foo", "bar"),
      "def" -> Seq("123")
      ), Map.empty[String, Seq[Multipart.FileUpload]]))
    )
  }

  test("Attribute") {
    assert(newRequest(Buf.Empty).multipart.get.attributes("type").head == "text")
  }

  test("FileUpload (in-memory)") {
    val foo = Buf.Utf8("foo")
    val multipart = newRequest(foo).multipart.get

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
    val foo = Buf.Utf8("." * (Multipart.MaxInMemoryFileSize.inBytes.toInt + 10))
    val multipart = newRequest(foo).multipart.get

    val Multipart.OnDiskFileUpload(file, contentType, fileName, contentTransferEncoding) =
      multipart.files("groups").head
    val attr = multipart.attributes("type").head

    assert(Buf.ByteArray.Owned(Files.readBytes(file, limit = Int.MaxValue)) == foo)
    assert(contentType == "image/gif")
    assert(fileName == "dealwithit.gif")
    assert(contentTransferEncoding == "binary")
    assert(attr == "text")
  }
}
