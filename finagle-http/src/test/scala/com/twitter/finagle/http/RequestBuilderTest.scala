package com.twitter.finagle.http

import com.twitter.io.Buf
import java.net.URL
import org.scalatest.funsuite.AnyFunSuite

class RequestBuilderTest extends AnyFunSuite {
  val URL0 = new URL("http://joe:blow@www.google.com:77/xxx?foo=bar#xxx")
  val URL1 = new URL("https://www.google.com/")
  val URL2 = new URL("http://joe%40host.com:blow@www.google.com:77/xxx?foo=bar#xxx")
  val URL3 = new URL("https://foo_bar_prod.www.address.com")

  val BODY0 = Buf.Utf8("blah")
  val FORM0 = Seq(
    "k1" -> "v",
    "k2" -> "v2",
    "k3" -> "v33"
  )

  val MULTIPART0 =
    """--Boundary
content-disposition: form-data; name="k1"
content-length: 1
content-type: text/plain; charset=UTF-8

v
--Boundary
content-disposition: form-data; name="k2"
content-length: 2
content-type: text/plain; charset=UTF-8

v2
--Boundary
content-disposition: form-data; name="k3"
content-length: 3
content-type: text/plain; charset=UTF-8

v33
--Boundary--
""".replace("\r\n", "\n")

  val MULTIPART1 =
    """--Boundary
content-disposition: form-data; name="foo"; filename="file-name"
content-length: 6
content-type: application/json; charset=UTF-8

foobar
--Boundary--
""".replace("\r\n", "\n")

  val FORMPOST0 = "k1=v&k2=v2&k3=v33"

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
    assert(get.headerMap(Fields.Host) == expected)
    assert(head.headerMap(Fields.Host) == expected)
    assert(delete.headerMap(Fields.Host) == expected)
    assert(put.headerMap(Fields.Host) == expected)
    assert(post.headerMap(Fields.Host) == expected)
  }

  test("set the HOST header when the HOST contains underscores") {
    val get = RequestBuilder().url(URL3).buildGet
    val head = RequestBuilder().url(URL3).buildHead
    val delete = RequestBuilder().url(URL3).buildDelete
    val put = RequestBuilder().url(URL3).buildPut(BODY0)
    val post = RequestBuilder().url(URL3).buildPost(BODY0)

    val expected = "foo_bar_prod.www.address.com"
    assert(get.headerMap(Fields.Host) == expected)
    assert(head.headerMap(Fields.Host) == expected)
    assert(delete.headerMap(Fields.Host) == expected)
    assert(put.headerMap(Fields.Host) == expected)
    assert(post.headerMap(Fields.Host) == expected)
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
    assert(req0.headerMap.get(Fields.Authorization) == Some(expected))
    assert(req1.headerMap.get(Fields.Authorization) == None)
    assert(req2.headerMap.get(Fields.Authorization) == Some(expected))
    assert(req3.headerMap.get(Fields.Authorization) == None)
    assert(req4.headerMap.get(Fields.Authorization) == Some(expected))
    assert(req5.headerMap.get(Fields.Authorization) == None)
    assert(req6.headerMap.get(Fields.Authorization) == Some(expected))
    assert(req7.headerMap.get(Fields.Authorization) == None)
    assert(req8.headerMap.get(Fields.Authorization) == Some(expected))
    assert(req9.headerMap.get(Fields.Authorization) == None)
    assert(req10.headerMap.get(Fields.Authorization) == Some(expectedSpecial))
  }

  test("replace the empty path with /") {
    val req0 = RequestBuilder().url(new URL("http://a.com")).buildGet
    val req1 = RequestBuilder().url(new URL("http://a.com?xxx")).buildGet

    req0.uri == "/"
    req1.uri == "/?xxx"
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

    assert(get0.uri == "/")
    assert(get1.uri == "/")
    assert(get2.uri == "/?abc=def")
    assert(head0.uri == "/")
    assert(head1.uri == "/")
    assert(head2.uri == "/?abc=def")
    assert(del0.uri == "/")
    assert(del1.uri == "/")
    assert(del2.uri == "/?abc=def")
    assert(put0.uri == "/")
    assert(put1.uri == "/")
    assert(put2.uri == "/?abc=def")
    assert(post0.uri == "/")
    assert(post1.uri == "/")
    assert(post2.uri == "/?abc=def")
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

    assert(get0.uri == "/")
    assert(get1.uri == "/")
    assert(get2.uri == "/")
    assert(head0.uri == "/")
    assert(head1.uri == "/")
    assert(head2.uri == "/")
    assert(del0.uri == "/")
    assert(del1.uri == "/")
    assert(del2.uri == "/")
    assert(put0.uri == "/")
    assert(put1.uri == "/")
    assert(put2.uri == "/")
    assert(post0.uri == "/")
    assert(post1.uri == "/")
    assert(post2.uri == "/")
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

    assert(builder0.buildGet.headerMap(A) == B)
    assert(builder0.buildHead.headerMap(A) == B)
    assert(builder0.buildDelete.headerMap(A) == B)
    assert(builder0.buildPut(BODY0).headerMap(A) == B)
    assert(builder0.buildPost(BODY0).headerMap(A) == B)

    val builder1 = builder0
      .setHeader(A, C)

    assert(builder1.buildGet.headerMap(A) == C)
    assert(builder1.buildHead.headerMap(A) == C)
    assert(builder1.buildDelete.headerMap(A) == C)
    assert(builder1.buildPut(BODY0).headerMap(A) == C)
    assert(builder1.buildPost(BODY0).headerMap(A) == C)

    val builder2 = builder1
      .setHeader(A, Seq())

    assert(builder2.buildGet.headerMap.get(A) == None)
    assert(builder2.buildHead.headerMap.get(A) == None)
    assert(builder2.buildDelete.headerMap.get(A) == None)
    assert(builder2.buildPut(BODY0).headerMap.get(A) == None)
    assert(builder2.buildPost(BODY0).headerMap.get(A) == None)

    val builder3 = builder2
      .setHeader(A, Seq(B, C))

    val pair = Seq(B, C)
    assert(builder3.buildGet.headerMap.getAll(A) == pair)
    assert(builder3.buildHead.headerMap.getAll(A) == pair)
    assert(builder3.buildDelete.headerMap.getAll(A) == pair)
    assert(builder3.buildPut(BODY0).headerMap.getAll(A) == pair)
    assert(builder3.buildPost(BODY0).headerMap.getAll(A) == pair)

    val builder4 = builder3
      .addHeader(A, D)

    val triple = Seq(B, C, D)
    assert(builder4.buildGet.headerMap.getAll(A) == triple)
    assert(builder4.buildHead.headerMap.getAll(A) == triple)
    assert(builder4.buildDelete.headerMap.getAll(A) == triple)
    assert(builder4.buildPut(BODY0).headerMap.getAll(A) == triple)
    assert(builder4.buildPost(BODY0).headerMap.getAll(A) == triple)
  }

  test("headers should be case insensitive") {
    val key1 = "KEY"
    val key2 = "key"
    val A = "A"
    val B = "B"
    val builder0 = RequestBuilder()
      .url(URL0)
      .setHeader(key1, Seq(A))
      .setHeader(key2, Seq(A, B))

    val pair = Seq(A, B)
    assert(builder0.buildGet.headerMap.getAll(key1) == pair)
    assert(builder0.buildGet.headerMap.getAll(key2) == pair)
    assert(builder0.buildHead.headerMap.getAll(key1) == pair)
    assert(builder0.buildHead.headerMap.getAll(key2) == pair)
    assert(builder0.buildDelete.headerMap.getAll(key1) == pair)
    assert(builder0.buildDelete.headerMap.getAll(key2) == pair)
    assert(builder0.buildPut(BODY0).headerMap.getAll(key1) == pair)
    assert(builder0.buildPut(BODY0).headerMap.getAll(key2) == pair)
    assert(builder0.buildPost(BODY0).headerMap.getAll(key1) == pair)
    assert(builder0.buildPost(BODY0).headerMap.getAll(key2) == pair)
  }

  test("build form") {
    val builder0 = RequestBuilder()
      .url(URL0)
      .addFormElement(FORM0: _*)

    val req0 = builder0.buildFormPost(false)
    val content = req0.contentString.replace("\r\n", "\n")
    assert(content == FORMPOST0)
  }

  test("build multipart form") {
    val builder0 = RequestBuilder()
      .url(URL0)
      .addFormElement(FORM0: _*)

    val req0 = builder0.buildFormPost(true)
    val content = "--[^-\r\n]+".r
      .replaceAllIn(req0.contentString, "--Boundary")
      .replace("\r\n", "\n")

    assert(content == MULTIPART0)
  }

  test("build multipart form with files") {
    val builder0 = RequestBuilder()
      .url(URL0)
      .add(
        FileElement("foo", Buf.Utf8("foobar"), Some("application/json"), Some("file-name"), true))

    val req0 = builder0.buildFormPost(true)
    val content = "--[^-\r\n]+".r
      .replaceAllIn(req0.contentString, "--Boundary")
      .replace("\r\n", "\n")

    assert(content == MULTIPART1)
  }
}
