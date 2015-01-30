package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest=>DefaultHttpAsk, HttpMethod, HttpVersion}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AskTest extends FunSuite {
  test("constructors") {
    val nettyAsk = new DefaultHttpAsk(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

    Seq(Ask(nettyAsk),
      Ask(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"),
      Ask(HttpMethod.GET, "/"),
      Ask("/"),
      Ask("/", "q" -> "twitter"),
      Ask("q" -> "twitter")
    ).foreach { request =>
      assert(request.version    === HttpVersion.HTTP_1_1)
      assert(request.method     === HttpMethod.GET)
      assert(request.path       === "/")
      assert(request.remoteHost === "127.0.0.1")
      assert(request.remotePort === 12345)
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
    tests.foreach { case (input, expected) => assert(Ask(input).path === expected) }
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
    tests.foreach { case (input, expected) => assert(Ask(input).fileExtension === expected) }
  }

  test("response") {
    val request = Ask("/search.json", "q" -> "twitter")
    val response = request.response

    assert(response.version === Version.Http11)
    assert(response.status  === Status.Ok)
  }

  test("toHttpString") {
    val request = Ask("/search.json", "q" -> "twitter")
    request.headers.set("Host", "search.twitter.com")

    val expected = "GET /search.json?q=twitter HTTP/1.1\r\nHost: search.twitter.com\r\n\r\n"

    val actual = request.encodeString()
    assert(actual === expected)
  }

  test("decode") {
    val request = Ask.decodeString(
      "GET /search.json?q=twitter HTTP/1.1\r\nHost: search.twitter.com\r\n\r\n")
    assert(request.path                === "/search.json")
    assert(request.params("q")         === "twitter")
    assert(request.headers.get("Host") === "search.twitter.com")
  }

  test("decodeBytes") {
    val originalAsk = Ask("/", "foo" -> "bar")
    val bytes = originalAsk.encodeBytes()
    val decodedAsk = Ask.decodeBytes(bytes)

    assert(decodedAsk.path          === "/")
    assert(decodedAsk.params("foo") === "bar")
  }

  test("queryString") {
    assert(Ask.queryString()                                          === "")
    assert(Ask.queryString(Map.empty[String, String])                 === "")
    assert(Ask.queryString("/search.json")                            === "/search.json")
    assert(Ask.queryString("/search.json", Map.empty[String, String]) === "/search.json")

    assert(Ask.queryString("/search.json", "q" -> "twitter")      === "/search.json?q=twitter")
    assert(Ask.queryString("/search.json", Map("q" -> "twitter")) === "/search.json?q=twitter")
    assert(Ask.queryString("q" -> "twitter")                      === "?q=twitter")
    assert(Ask.queryString(Map("q" -> "twitter"))                 === "?q=twitter")

    assert(Ask.queryString("q!" -> "twitter!") === "?q%21=twitter%21")
  }
}
