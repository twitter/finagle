package com.twitter.finagle.http

import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class RequestTest extends AnyFunSuite {
  test("constructors") {
    Seq(
      Request(),
      Request(Version.Http11, Method.Get, "/"),
      Request(Method.Get, "/"),
      Request("/"),
      Request("/", "q" -> "twitter"),
      Request("q" -> "twitter")
    ).foreach { request =>
      assert(request.version == Version.Http11)
      assert(request.method == Method.Get)
      assert(request.path == "/")
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
      "/search" -> "",
      "/search." -> "",
      "/" -> "",
      "/." -> ""
    )
    tests.foreach { case (input, expected) => assert(Request(input).fileExtension == expected) }
  }

  test("response") {
    val request = Request("/search.json", "q" -> "twitter")
    val response = request.response

    assert(response.version == Version.Http11)
    assert(response.status == Status.Ok)
  }

  test("uri") {
    val req = Request("/foo1")
    assert(req.uri == "/foo1")

    req.uri = "/foo2"
    assert(req.uri == "/foo2")

    req.uri("/foo3")
    assert(req.uri == "/foo3")
  }

  test("queryString") {
    assert(Request.queryString() == "")
    assert(Request.queryString(Map.empty[String, String]) == "")
    assert(Request.queryString("/search.json") == "/search.json")
    assert(Request.queryString("/search.json", Map.empty[String, String]) == "/search.json")

    assert(Request.queryString("/search.json", "q" -> "twitter") == "/search.json?q=twitter")
    assert(Request.queryString("/search.json", Map("q" -> "twitter")) == "/search.json?q=twitter")
    assert(Request.queryString("q" -> "twitter") == "?q=twitter")
    assert(Request.queryString(Map("q" -> "twitter")) == "?q=twitter")

    assert(Request.queryString("q!" -> "twitter!") == "?q%21=twitter%21")
  }

  test("getParam") {
    val request = Request(Method.Post, "/search")
    request.mediaType = MediaType.WwwForm
    request.contentString = "q=twitter"
    assert(request.getParam("q") == "twitter")
    assert(request.getParam("r") == null)

    assert(request.getParam("q", "myDefault") == "twitter")
    assert(request.getParam("r", "myDefault") == "myDefault")
  }

  test("getShortParam") {
    val request = Request(Method.Post, "/search")
    request.mediaType = MediaType.WwwForm
    request.contentString = "q=10&r=x"
    assert(request.getShortParam("q") == 10)
    assert(request.getShortParam("r") == 0)
    assert(request.getShortParam("s") == 0)

    assert(request.getShortParam("q", 11) == 10)
    assert(request.getShortParam("r", 11) == 0)
    assert(request.getShortParam("s", 11) == 11)
  }

  test("getIntParam") {
    val request = Request(Method.Post, "/search")
    request.mediaType = MediaType.WwwForm
    request.contentString = "q=10&r=x"
    assert(request.getIntParam("q") == 10)
    assert(request.getIntParam("r") == 0)
    assert(request.getIntParam("s") == 0)

    assert(request.getIntParam("q", 11) == 10)
    assert(request.getIntParam("r", 11) == 0)
    assert(request.getIntParam("s", 11) == 11)
  }

  test("getLongParam") {
    val request = Request(Method.Post, "/search")
    request.mediaType = MediaType.WwwForm
    request.contentString = "q=10&r=x"
    assert(request.getLongParam("q") == 10)
    assert(request.getLongParam("r") == 0)
    assert(request.getLongParam("s") == 0)

    assert(request.getLongParam("q", 11) == 10)
    assert(request.getLongParam("r", 11) == 0)
    assert(request.getLongParam("s", 11) == 11)
  }

  test("getBooleanParam") {
    val request = Request(Method.Post, "/search")
    request.mediaType = MediaType.WwwForm
    request.contentString = "q=1&r=x"
    assert(request.getBooleanParam("q"))
    assert(!request.getBooleanParam("r"))
    assert(!request.getBooleanParam("s"))

    assert(request.getBooleanParam("q", true))
    assert(!request.getBooleanParam("r", true))
    assert(request.getBooleanParam("s", true))
  }

  test("getParams") {
    val request = Request(Method.Post, "/search?r=yes")
    request.mediaType = MediaType.WwwForm
    request.contentString = "q=1&r=x"

    val result = request.getParams("r")
    assert(result == List("x", "yes").asJava)

    val all = request.getParams()
    assert(all.size == 3)
  }

  test("containsParam") {
    val request = Request(Method.Post, "/search?r=yes")
    request.mediaType = MediaType.WwwForm
    request.contentString = "q=1&s=x"

    assert(request.containsParam("r"))
    assert(request.containsParam("s"))
    assert(!request.containsParam("t"))
  }

  test("getParamNames") {
    val request = Request(Method.Post, "/search?r=yes")
    request.mediaType = MediaType.WwwForm
    request.contentString = "q=1&s=x"

    val names = request.getParamNames()
    assert(names.size == 3)
  }

}
