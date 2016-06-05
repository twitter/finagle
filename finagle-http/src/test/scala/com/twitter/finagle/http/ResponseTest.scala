package com.twitter.finagle.http

import com.twitter.io.Charsets
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResponseTest extends FunSuite {
  test("constructors") {
    List(
      Response(),
      Response(Version.Http11, Status.Ok),
      Response()
    ).foreach { response =>
      assert(response.version == Version.Http11)
      assert(response.status == Status.Ok)
    }
  }

  test("encode") {
    val response = Response()
    response.headerMap.set("Server", "macaw")

    val expected = "HTTP/1.1 200 OK\r\nServer: macaw\r\n\r\n"
    val actual = response.encodeString()

    assert(actual == expected)
  }

  test("decodeString") {
    val response = Response.decodeString(
      "HTTP/1.1 200 OK\r\nServer: macaw\r\nContent-Length: 0\r\n\r\n")

    assert(response.status == Status.Ok)
    assert(response.headerMap(Fields.Server) == "macaw")
  }

  test("decodeBytes") {
    val response = Response.decodeBytes(
      "HTTP/1.1 200 OK\r\nServer: macaw\r\nContent-Length: 0\r\n\r\n".getBytes(Charsets.Utf8))

    assert(response.status == Status.Ok)
    assert(response.headerMap(Fields.Server) == "macaw")
  }

}
