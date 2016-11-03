package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RequestTest extends FunSuite {
  test("constructors") {
    Seq(Request(),
      Request(Version.Http11, Method.Get, "/"),
      Request(Method.Get, "/"),
      Request("/"),
      Request("/", "q" -> "twitter"),
      Request("q" -> "twitter")
    ).foreach { request =>
      assert(request.version    == Version.Http11)
      assert(request.method     == Method.Get)
      assert(request.path       == "/")
    }
  }

  test("path") {
    val tests = Map(
      "/" -> "/",
      "/search.json" -> "/search.json",
      "/search.json?" -> "/search.json",
      "/search.json?q=twitter" -> "/search.json",
      "/search.json%3Fq=twitter" -> "/search.json%3Fq=twitter"
    )
    tests.foreach { case (input, expected) => assert(Request(input).path == expected) }
  }

  test("file extension") {
    val tests = Map(
      "/search.json" -> "json",
      "/1.1/search/tweets.json" -> "json",
      "/1.1/search/tweets.JSON" -> "json",
      "/1.1/search/tweets" -> "",
      "/1.1/se.arch/tweets" -> "",
      "/1.1/se.arch/tweets.json" -> "json",
      "/search"      -> "",
      "/search."     -> "",
      "/"            -> "",
      "/."           -> ""
    )
    tests.foreach { case (input, expected) => assert(Request(input).fileExtension == expected) }
  }

  test("response") {
    val request = Request("/search.json", "q" -> "twitter")
    val response = request.response

    assert(response.version == Version.Http11)
    assert(response.status  == Status.Ok)
  }

  test("toHttpString") {
    val request = Request("/search.json", "q" -> "twitter")
    request.headers.set("Host", "search.twitter.com")

    val expected = "GET /search.json?q=twitter HTTP/1.1\r\nHost: search.twitter.com\r\n\r\n"

    val actual = request.encodeString()
    assert(actual == expected)
  }

  test("decode") {
    val request = Request.decodeString(
      "GET /search.json?q=twitter HTTP/1.1\r\nHost: search.twitter.com\r\n\r\n")
    assert(request.path                == "/search.json")
    assert(request.params("q")         == "twitter")
    assert(request.headers.get("Host") == "search.twitter.com")
  }

  test("decodeBytes") {
    val originalRequest = Request("/", "foo" -> "bar")
    val bytes = originalRequest.encodeBytes()
    val decodedRequest = Request.decodeBytes(bytes)

    assert(decodedRequest.path          == "/")
    assert(decodedRequest.params("foo") == "bar")
  }

  test("queryString") {
    assert(Request.queryString()                                          == "")
    assert(Request.queryString(Map.empty[String, String])                 == "")
    assert(Request.queryString("/search.json")                            == "/search.json")
    assert(Request.queryString("/search.json", Map.empty[String, String]) == "/search.json")

    assert(Request.queryString("/search.json", "q" -> "twitter")      == "/search.json?q=twitter")
    assert(Request.queryString("/search.json", Map("q" -> "twitter")) == "/search.json?q=twitter")
    assert(Request.queryString("q" -> "twitter")                      == "?q=twitter")
    assert(Request.queryString(Map("q" -> "twitter"))                 == "?q=twitter")

    assert(Request.queryString("q!" -> "twitter!") == "?q%21=twitter%21")
  }
}
