package com.twitter.finagle.http.codec

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.http.Fields
import com.twitter.finagle.http.Method
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Status
import com.twitter.finagle.http.Version
import com.twitter.finagle.http.Status._
import com.twitter.io.Buf
import com.twitter.io.ReaderDiscardedException
import com.twitter.util.Await
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class ResponseConformanceFilterTest extends AnyFunSuite {

  test("add content-length header") {
    val resp = Response()
    val body = Buf.Utf8("a body")
    resp.content = body

    val resp2 = fetchResponse(resp)
    assert(resp2.contentLength == Some(body.length.toLong))
  }

  test(
    "doesn't strip content-length header from chunked response " +
      "if 'transfer-encoding: chunked' is not set"
  ) {
    val resp = Response()
    resp.setChunked(true)
    resp.contentLength = 45

    val resp2 = fetchResponse(resp)
    assert(resp2.contentLength == Some(45))
    assert(!resp2.headerMap.contains(Fields.TransferEncoding))
  }

  test(
    "strip content-length header from chunked response " +
      "if 'transfer-encoding: chunked' is set"
  ) {
    val resp = Response()
    resp.setChunked(true)
    resp.headerMap.set(Fields.TransferEncoding, "chunked")
    resp.contentLength = 45

    val resp2 = fetchResponse(resp)
    assert(resp2.contentLength == None)
    assert(resp2.headerMap.contains(Fields.TransferEncoding))
  }

  test("transfer-encoding not added to HTTP/1.0 responses") {
    val resp = Response()
    resp.setChunked(true)
    resp.version = Version.Http10
    val resp2 = fetchResponse(resp)
    assert(!resp2.headerMap.contains(Fields.TransferEncoding))
  }

  test("transfer-encoding is stripped from HTTP/1.0 responses") {
    val resp = Response()
    resp.setChunked(true)
    resp.version = Version.Http10
    resp.headerMap.add(Fields.TransferEncoding, "chunked")
    val resp2 = fetchResponse(resp)
    assert(!resp2.headerMap.contains(Fields.TransferEncoding))
  }

  test("Doesn't clobber non-chunked values for Transfer-Encoding headers") {
    val resp = Response()
    resp.setChunked(true)
    resp.headerMap.add(Fields.TransferEncoding, "gzip")
    val resp2 = fetchResponse(resp)
    val teHeaders = resp2.headerMap.getAll(Fields.TransferEncoding).toSet

    assert(teHeaders == Set("gzip", "chunked"))
  }

  test(
    "Doesn't remove non-chunked values for Transfer-Encoding headers when " +
      "Content-Length header is present"
  ) {
    val resp = Response()
    resp.setChunked(true)
    resp.headerMap.add(Fields.TransferEncoding, "gzip")
    resp.contentLength = 45
    val resp2 = fetchResponse(resp)

    assert(resp2.headerMap.getAll(Fields.TransferEncoding) == Seq("gzip"))
    assert(resp.contentLength == Some(45))
  }

  test("response to HEAD request with content-length") {
    val res = Response()
    res.contentLength = 1

    val resp = fetchHeadResponse(res)
    assert(resp.status == Status.Ok)
    assert(resp.content.length == 0)
    assert(!resp.isChunked)
    assert(resp.headerMap.get(Fields.ContentLength) == Some("1"))
  }

  test("response to HEAD request without content-length") {
    val response = fetchHeadResponse(Response())
    assert(response.status == Status.Ok)
    assert(response.content.length == 0)
    assert(!response.isChunked)
    assert(response.headerMap.get(Fields.ContentLength) == None)
  }

  test("response to HEAD request that contains a body") {
    val body = Buf.Utf8("some data")
    val res = Response()
    res.content = body

    val response = fetchHeadResponse(res)
    assert(response.status == Status.Ok)
    assert(response.content.length == 0)
    assert(!response.isChunked)
    assert(response.headerMap.get(Fields.ContentLength) == Some(body.length.toString))
  }

  test("response to HEAD request with chunked response lacking a body") {
    val res = Response()
    res.setChunked(true)

    val response = fetchHeadResponse(res)

    assert(response.status == Status.Ok)
    assert(response.content.length == 0)
    assert(!response.isChunked) // the pipeline will clear the chunked flag
    assert(response.headerMap.get(Fields.ContentLength) == None)

    // Make sure to close the Reader/Writer pair, just in case someone is listening
    intercept[ReaderDiscardedException] { Await.result(res.writer.write(Buf.Empty), 5.seconds) }
  }

  List(Continue, SwitchingProtocols, Processing, NoContent, NotModified).foreach { status =>
    test(
      s"response with status code ${status.code} must not have a message body nor " +
        "Content-Length header field"
    ) {
      val res = Response(Version.Http11, status)
      val response = fetchResponse(res)

      assert(response.status == status)
      assert(response.length == 0)
      assert(!response.isChunked)
      assert(response.headerMap.get(Fields.ContentLength).isEmpty)
    }
  }

  List(Continue, SwitchingProtocols, Processing, NoContent, NotModified).foreach { status =>
    test(
      s"response with status code ${status.code} must not have a message body nor " +
        "Content-Length header field when non-empty body is returned"
    ) {
      val body = Buf.Utf8("some data")
      val res = Response(Version.Http11, status)
      res.content = body

      val response = fetchResponse(res)

      assert(response.status == status)
      assert(response.length == 0)
      assert(!response.isChunked)
      assert(response.headerMap.get(Fields.ContentLength).isEmpty)
    }
  }

  List(Continue, SwitchingProtocols, Processing, NoContent).foreach { status =>
    test(
      s"response with status code ${status.code} must not have a message body nor " +
        "Content-Length header field when non-empty body with explicit Content-Length is returned"
    ) {
      val body = Buf.Utf8("some data")
      val res = Response(Version.Http11, status)
      res.content = body
      res.contentLength = body.length.toLong

      val response = fetchResponse(res)

      assert(response.status == status)
      assert(response.length == 0)
      assert(!response.isChunked)
      assert(response.headerMap.get(Fields.ContentLength).isEmpty)
    }
  }

  test(
    "response with status code 304 must not have a message body *BUT* Content-Length " +
      "header field when non-empty body with explicit Content-Length is returned"
  ) {
    val body = Buf.Utf8("some data")
    val res = Response(Version.Http11, Status.NotModified)
    res.content = body
    res.contentLength = body.length.toLong

    val response = fetchResponse(res)

    assert(response.status == Status.NotModified)
    assert(response.length == 0)
    assert(!response.isChunked)
    assert(response.headerMap.get(Fields.ContentLength).contains(body.length.toString))
  }

  def fetchResponse(res: Response): Response = {
    val request = Request(uri = "/")
    runFilter(request, res)
  }

  // Helper method for handling HEAD requests
  def fetchHeadResponse(res: Response): Response = {
    val request = Request(uri = "/", method = Method.Head)
    runFilter(request, res)
  }

  def runFilter(req: Request, res: Response): Response = {
    val service = ResponseConformanceFilter andThen Service.mk { _: Request => Future.value(res) }
    Await.result(service(req), 5.seconds)
  }

}
