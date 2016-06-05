package com.twitter.finagle.http

import com.twitter.io.Buf
import java.net.URL
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class RequestBuilderTest extends FunSuite {
  val URL0 = new URL("http://joe:blow@www.google.com:77/xxx?foo=bar#xxx")
  val URL1 = new URL("https://www.google.com/")
  val URL2 = new URL("http://joe%40host.com:blow@www.google.com:77/xxx?foo=bar#xxx")

  val BODY0 = Buf.Utf8("blah")
  val FORM0 = Seq (
    "k1" -> "v1",
    "k2" -> "v2",
    "k3" -> "v3"
  )

  val MULTIPART0 =
    """--Boundary
Content-Disposition: form-data; name="k1"
Content-Type: text/plain; charset=UTF-8

v1
--Boundary
Content-Disposition: form-data; name="k2"
Content-Type: text/plain; charset=UTF-8

v2
--Boundary
Content-Disposition: form-data; name="k3"
Content-Type: text/plain; charset=UTF-8

v3
--Boundary--
""".replace("\r\n", "\n")

  val FORMPOST0 = "k1=v1&k2=v2&k3=v3"

  test("reject non-http urls") {
    intercept[IllegalArgumentException] {
      RequestBuilder().url(new URL("ftp:///blah"))
    }
  }

  test("set the HOST header") {
    val get = RequestBuilder().url(URL0).buildGet
    val head = RequestBuilder().url(URL0).buildHead
    val delete = RequestBuilder().url(URL0).buildDelete
    val put = RequestBuilder().url(URL0).buildPut(BODY0)
    val post = RequestBuilder().url(URL0).buildPost(BODY0)

    val expected = "www.google.com:77"
    assert(get.headers.get(Fields.Host) == expected)
    assert(head.headers.get(Fields.Host) == expected)
    assert(delete.headers.get(Fields.Host) == expected)
    assert(put.headers.get(Fields.Host) == expected)
    assert(post.headers.get(Fields.Host) == expected)
  }

  test("set the Authorization header when userInfo is present") {
    val req0 = RequestBuilder().url(URL0).buildGet
    val req1 = RequestBuilder().url(URL1).buildGet
    val req2 = RequestBuilder().url(URL0).buildHead
    val req3 = RequestBuilder().url(URL1).buildHead
    val req4 = RequestBuilder().url(URL0).buildDelete
    val req5 = RequestBuilder().url(URL1).buildDelete
    val req6 = RequestBuilder().url(URL0).buildPut(BODY0)
    val req7 = RequestBuilder().url(URL1).buildPut(BODY0)
    val req8 = RequestBuilder().url(URL0).buildPost(BODY0)
    val req9 = RequestBuilder().url(URL1).buildPost(BODY0)
    val req10 = RequestBuilder().url(URL2).buildPost(BODY0)

    val expected = "Basic am9lOmJsb3c="
    val expectedSpecial = "Basic am9lQGhvc3QuY29tOmJsb3c="
    assert(req0.headers.get(Fields.Authorization) == expected)
    assert(req1.headers.get(Fields.Authorization) == null)
    assert(req2.headers.get(Fields.Authorization) == expected)
    assert(req3.headers.get(Fields.Authorization) == null)
    assert(req4.headers.get(Fields.Authorization) == expected)
    assert(req5.headers.get(Fields.Authorization) == null)
    assert(req6.headers.get(Fields.Authorization) == expected)
    assert(req7.headers.get(Fields.Authorization) == null)
    assert(req8.headers.get(Fields.Authorization) == expected)
    assert(req9.headers.get(Fields.Authorization) == null)
    assert(req10.headers.get(Fields.Authorization) == expectedSpecial)
  }

  test("replace the empty path with /") {
    val req0 = RequestBuilder().url(new URL("http://a.com")).buildGet
    val req1 = RequestBuilder().url(new URL("http://a.com?xxx")).buildGet

    req0.getUri == "/"
    req1.getUri == "/?xxx"
  }

  test("not include the fragment in the resource") {
    val u0 = new URL("http://a.com#xxx")
    val u1 = new URL("http://a.com/#xxx")
    val u2 = new URL("http://a.com/?abc=def#xxx")

    val get0 = RequestBuilder().url(u0).buildGet
    val get1 = RequestBuilder().url(u1).buildGet
    val get2 = RequestBuilder().url(u2).buildGet
    val head0 = RequestBuilder().url(u0).buildHead
    val head1 = RequestBuilder().url(u1).buildHead
    val head2 = RequestBuilder().url(u2).buildHead
    val del0 = RequestBuilder().url(u0).buildDelete
    val del1 = RequestBuilder().url(u1).buildDelete
    val del2 = RequestBuilder().url(u2).buildDelete
    val put0 = RequestBuilder().url(u0).buildPut(BODY0)
    val put1 = RequestBuilder().url(u1).buildPut(BODY0)
    val put2 = RequestBuilder().url(u2).buildPut(BODY0)
    val post0 = RequestBuilder().url(u0).buildPost(BODY0)
    val post1 = RequestBuilder().url(u1).buildPost(BODY0)
    val post2 = RequestBuilder().url(u2).buildPost(BODY0)

    assert(get0.getUri == "/")
    assert(get1.getUri == "/")
    assert(get2.getUri == "/?abc=def")
    assert(head0.getUri == "/")
    assert(head1.getUri == "/")
    assert(head2.getUri == "/?abc=def")
    assert(del0.getUri == "/")
    assert(del1.getUri == "/")
    assert(del2.getUri == "/?abc=def")
    assert(put0.getUri == "/")
    assert(put1.getUri == "/")
    assert(put2.getUri == "/?abc=def")
    assert(post0.getUri == "/")
    assert(post1.getUri == "/")
    assert(post2.getUri == "/?abc=def")
  }

  test("not include the empty query string") {
    val u0 = new URL("http://a.com?")
    val u1 = new URL("http://a.com/?")
    val u2 = new URL("http://a.com/?#xxx")

    val get0 = RequestBuilder().url(u0).buildGet
    val get1 = RequestBuilder().url(u1).buildGet
    val get2 = RequestBuilder().url(u2).buildGet
    val head0 = RequestBuilder().url(u0).buildHead
    val head1 = RequestBuilder().url(u1).buildHead
    val head2 = RequestBuilder().url(u2).buildHead
    val del0 = RequestBuilder().url(u0).buildDelete
    val del1 = RequestBuilder().url(u1).buildDelete
    val del2 = RequestBuilder().url(u2).buildDelete
    val put0 = RequestBuilder().url(u0).buildPut(BODY0)
    val put1 = RequestBuilder().url(u1).buildPut(BODY0)
    val put2 = RequestBuilder().url(u2).buildPut(BODY0)
    val post0 = RequestBuilder().url(u0).buildPost(BODY0)
    val post1 = RequestBuilder().url(u1).buildPost(BODY0)
    val post2 = RequestBuilder().url(u2).buildPost(BODY0)

    assert(get0.getUri == "/")
    assert(get1.getUri == "/")
    assert(get2.getUri == "/")
    assert(head0.getUri == "/")
    assert(head1.getUri == "/")
    assert(head2.getUri == "/")
    assert(del0.getUri == "/")
    assert(del1.getUri == "/")
    assert(del2.getUri == "/")
    assert(put0.getUri == "/")
    assert(put1.getUri == "/")
    assert(put2.getUri == "/")
    assert(post0.getUri == "/")
    assert(post1.getUri == "/")
    assert(post2.getUri == "/")
  }

  test("set the correct protocol version") {
    val get0 = RequestBuilder().url(URL0).buildGet
    val get1 = RequestBuilder().url(URL0).http10.buildGet
    val head0 = RequestBuilder().url(URL0).buildHead
    val head1 = RequestBuilder().url(URL0).http10.buildHead
    val del0 = RequestBuilder().url(URL0).buildDelete
    val del1 = RequestBuilder().url(URL0).http10.buildDelete
    val put0 = RequestBuilder().url(URL0).buildPut(BODY0)
    val put1 = RequestBuilder().url(URL0).http10.buildPut(BODY0)
    val post0 = RequestBuilder().url(URL0).buildPost(BODY0)
    val post1 = RequestBuilder().url(URL0).http10.buildPost(BODY0)

    assert(get0.version == Version.Http11)
    assert(get1.version == Version.Http10)
    assert(head0.version == Version.Http11)
    assert(head1.version == Version.Http10)
    assert(del0.version == Version.Http11)
    assert(del1.version == Version.Http10)
    assert(put0.version == Version.Http11)
    assert(put1.version == Version.Http10)
    assert(post0.version == Version.Http11)
    assert(post1.version == Version.Http10)
  }

  test("set the correct method") {
    val get = RequestBuilder().url(URL0).buildGet
    val head = RequestBuilder().url(URL0).buildHead
    val del = RequestBuilder().url(URL0).buildDelete
    val put = RequestBuilder().url(URL0).buildPut(BODY0)
    val post = RequestBuilder().url(URL0).buildPost(BODY0)

    assert(get.method == Method.Get)
    assert(head.method == Method.Head)
    assert(del.method == Method.Delete)
    assert(put.method == Method.Put)
    assert(post.method == Method.Post)
  }

  test("set headers") {
    val A = "A"
    val B = "B"
    val C = "C"
    val D = "D"
    val builder0 = RequestBuilder()
      .url(URL0)
      .setHeader(A, B)

    assert(builder0.buildGet.headers.get(A) == B)
    assert(builder0.buildHead.headers.get(A) == B)
    assert(builder0.buildDelete.headers.get(A) == B)
    assert(builder0.buildPut(BODY0).headers.get(A) == B)
    assert(builder0.buildPost(BODY0).headers.get(A) == B)

    val builder1 = builder0
      .setHeader(A, C)

    assert(builder1.buildGet.headers.get(A) == C)
    assert(builder1.buildHead.headers.get(A) == C)
    assert(builder1.buildDelete.headers.get(A) == C)
    assert(builder1.buildPut(BODY0).headers.get(A) == C)
    assert(builder1.buildPost(BODY0).headers.get(A) == C)

    val builder2 = builder1
      .setHeader(A, Seq())

    assert(builder2.buildGet.headers.get(A) == null)
    assert(builder2.buildHead.headers.get(A) == null)
    assert(builder2.buildDelete.headers.get(A) == null)
    assert(builder2.buildPut(BODY0).headers.get(A) == null)
    assert(builder2.buildPost(BODY0).headers.get(A) == null)

    val builder3 = builder2
      .setHeader(A, Seq(B, C))

    val pair = Seq(B,C).asJava
    assert(builder3.buildGet.headers.getAll(A) == pair)
    assert(builder3.buildHead.headers.getAll(A) == pair)
    assert(builder3.buildDelete.headers.getAll(A) == pair)
    assert(builder3.buildPut(BODY0).headers.getAll(A) == pair)
    assert(builder3.buildPost(BODY0).headers.getAll(A) == pair)

    val builder4 = builder3
      .addHeader(A, D)

    val triple = Seq(B,C, D).asJava
    assert(builder4.buildGet.headers.getAll(A) == triple)
    assert(builder4.buildHead.headers.getAll(A) == triple)
    assert(builder4.buildDelete.headers.getAll(A) == triple)
    assert(builder4.buildPut(BODY0).headers.getAll(A) == triple)
    assert(builder4.buildPost(BODY0).headers.getAll(A) == triple)
  }

  test("build form") {
    val builder0 = RequestBuilder()
      .url(URL0)
      .addFormElement(FORM0:_*)

    val req0 = builder0.buildFormPost(false)
    val content = req0.contentString.replace("\r\n", "\n")
    assert(content == FORMPOST0)
  }

  test("build multipart form") {
    val builder0 = RequestBuilder()
      .url(URL0)
      .addFormElement(FORM0:_*)

    val req0 = builder0.buildFormPost(true)
    val content = "--[^-\r\n]+".r.replaceAllIn(req0.contentString, "--Boundary")
      .replace("\r\n", "\n")
    assert(content == MULTIPART0)
  }
}
