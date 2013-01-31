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
  }
}
