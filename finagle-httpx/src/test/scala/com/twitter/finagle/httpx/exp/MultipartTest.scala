package com.twitter.finagle.httpx.exp

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.httpx.{FileElement, Request, RequestBuilder, SimpleElement}
import com.twitter.io.Buf

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
  private[this] def newRequest(): Request =
    RequestBuilder()
      .url("http://example.com")
      .add(FileElement("groups", Buf.Empty, Some("image/gif"), Some("dealwithit.gif")))
      .add(SimpleElement("type", "text"))
      .buildFormPost(multipart = true)

  test("sanity check") {
    val req = Request()
    req.contentString = "abc=foo&def=123&abc=bar"
    val multipart = Multipart.decodeNonChunked(req)

    assert(multipart == Multipart(Map(
      "abc" -> Seq("foo", "bar"),
      "def" -> Seq("123")
      ), Map.empty[String, Seq[Multipart.FileUpload]])
    )
  }

  test("Attribute") {
    val req = newRequest()
    val multipart =  Multipart.decodeNonChunked(req)
    val attr = multipart.attributes("type").head

    assert(attr == "text")
  }

  test("FileUpload") {
    val req = newRequest()
    val multipart = Multipart.decodeNonChunked(req)

    val file = multipart.files("groups").head
    val attr = multipart.attributes("type").head

    assert(file.contentType == "image/gif")
    assert(file.content.length == 0)
    assert(file.fileName == "dealwithit.gif")
    assert(file.contentTransferEncoding == "binary")

    assert(attr == "text")
  }
}
