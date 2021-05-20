package com.twitter.finagle.http.codec

import com.twitter.finagle.http.{Fields, Request, Response, Status}
import java.nio.charset.StandardCharsets.UTF_8
import org.scalatest.funsuite.AnyFunSuite

class HttpCodecTest extends AnyFunSuite {
  test("encodeRequestToString") {
    val request = Request("/search.json", "q" -> "twitter")
    request.headerMap.set("Host", "search.twitter.com")
    val expected = "GET /search.json?q=twitter HTTP/1.1\r\nHost: search.twitter.com\r\n\r\n"
    val actual = HttpCodec.encodeRequestToString(request)
    assert(actual == expected)
  }

  test("decodeStringToRequest") {
    val request = HttpCodec.decodeStringToRequest(
      "GET /search.json?q=twitter HTTP/1.1\r\nHost: search.twitter.com\r\n\r\n"
    )
    assert(request.path == "/search.json")
    assert(request.params("q") == "twitter")
    assert(request.headerMap.get("Host") == Some("search.twitter.com"))
  }

  test("decodeBytesToRequest") {
    val originalRequest = Request("/", "foo" -> "bar")
    val bytes = HttpCodec.encodeRequestToBytes(originalRequest)
    val decodedRequest = HttpCodec.decodeBytesToRequest(bytes)

    assert(decodedRequest.path == "/")
    assert(decodedRequest.params("foo") == "bar")
  }

  test("encodeResponseToString") {
    val response = Response()
    response.headerMap.set("Server", "macaw")

    val expected = "HTTP/1.1 200 OK\r\nServer: macaw\r\n\r\n"
    val actual = HttpCodec.encodeResponseToString(response)

    assert(actual == expected)
  }

  test("decodeStringToResponse") {
    val response = HttpCodec.decodeStringToResponse(
      "HTTP/1.1 200 OK\r\nServer: macaw\r\nContent-Length: 0\r\n\r\n"
    )

    assert(response.status == Status.Ok)
    assert(response.headerMap(Fields.Server) == "macaw")
  }

  test("decodeBytesToResponse") {
    val response = HttpCodec.decodeBytesToResponse(
      "HTTP/1.1 200 OK\r\nServer: macaw\r\nContent-Length: 0\r\n\r\n".getBytes(UTF_8)
    )

    assert(response.status == Status.Ok)
    assert(response.headerMap(Fields.Server) == "macaw")
  }
}
