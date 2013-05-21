package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpMethod, HttpVersion}
import org.specs.SpecificationWithJUnit
import org.specs.util.DataTables


class RequestSpec extends SpecificationWithJUnit with DataTables {
  "Request" should {
    "constructors" in {
      val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

      Seq(Request(nettyRequest),
          Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"),
          Request(HttpMethod.GET, "/"),
          Request("/"),
          Request("/", "q" -> "twitter"),
          Request("q" -> "twitter")
         ).foreach { request =>
        request.version    must_== HttpVersion.HTTP_1_1
        request.method     must_== HttpMethod.GET
        request.path       must_== "/"
        request.remoteHost must_== "127.0.0.1"
        request.remotePort must_== 12345
      }
    }

    "path" in {
      "uri"                      | "path"                     |>
      "/"                        ! "/"                        |
      "/search.json"             ! "/search.json"             |
      "/search.json?"            ! "/search.json"             |
      "/search.json?q=twitter"   ! "/search.json"             |
      "/search.json%3Fq=twitter" ! "/search.json%3Fq=twitter" |
      { (uri: String, path: String) =>
        Request(uri).path must_== path
      }
    }

    "file extension" in {
      "uri"                       | "extension" |>
      "/search.json"              ! "json"      |
      "/1.1/search/tweets.json"   ! "json"      |
      "/1.1/search/tweets.JSON"   ! "json"      |
      "/1.1/search/tweets"        ! ""          |
      "/1.1/se.arch/tweets"       ! ""          |
      "/1.1/se.arch/tweets.json"  ! "json"      |
      "/search"                   ! ""          |
      "/search."                  ! ""          |
      "/"                         ! ""          |
      "/."                        ! ""          |
      { (uri: String, extension: String) =>
        Request(uri).fileExtension must_== extension
      }
    }

    "response" in {
      val request = Request("/search.json", "q" -> "twitter")

      val response = request.response
      response.version must_== Version.Http11
      response.status  must_== Status.Ok
    }

    "toHttpString" in {
      val request = Request("/search.json", "q" -> "twitter")
      request.headers("Host") = "search.twitter.com"

      val expected = "GET /search.json?q=twitter HTTP/1.1\r\nHost: search.twitter.com\r\n\r\n"

      val actual = request.encodeString()
      actual must_== expected
    }

    "decode" in {
      val request = Request.decodeString(
        "GET /search.json?q=twitter HTTP/1.1\r\nHost: search.twitter.com\r\n\r\n")
      request.path            must_== "/search.json"
      request.params("q")     must_== "twitter"
      request.headers("Host") must_== "search.twitter.com"
    }

    "decodeBytes" in {
      val originalRequest = Request("/", "foo" -> "bar")
      val bytes = originalRequest.encodeBytes()
      val decodedRequest = Request.decodeBytes(bytes)

      decodedRequest.path          must_== "/"
      decodedRequest.params("foo") must_== "bar"
    }

    "queryString" in {
      Request.queryString()                                          must_== ""
      Request.queryString(Map.empty[String, String])                 must_== ""
      Request.queryString("/search.json")                            must_== "/search.json"
      Request.queryString("/search.json", Map.empty[String, String]) must_== "/search.json"

      Request.queryString("/search.json", "q" -> "twitter")      must_== "/search.json?q=twitter"
      Request.queryString("/search.json", Map("q" -> "twitter")) must_== "/search.json?q=twitter"
      Request.queryString("q" -> "twitter")                      must_== "?q=twitter"
      Request.queryString(Map("q" -> "twitter"))                 must_== "?q=twitter"

      Request.queryString("q!" -> "twitter!") must_== "?q%21=twitter%21"
    }
  }
}
