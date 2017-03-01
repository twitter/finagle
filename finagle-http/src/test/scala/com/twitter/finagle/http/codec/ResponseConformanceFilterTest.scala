package com.twitter.finagle.http.codec

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Fields, Method, Request, Response, Status}
import com.twitter.io.Buf
import com.twitter.io.Reader.ReaderDiscarded
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResponseConformanceFilterTest extends FunSuite {

  test("add content-length header") {
    val resp = Response()
    val body = Buf.Utf8("a body")
    resp.content = body

    val resp2 = fetchResponse(resp)
    assert(resp2.contentLength == Some(body.length.toLong))
  }

  test("strip content-length header from chunked response") {
    val resp = Response()
    resp.setChunked(true)
    resp.contentLength = 45

    val resp2 = fetchResponse(resp)
    assert(resp2.contentLength == None)
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
    intercept[ReaderDiscarded] { Await.result(res.writer.write(Buf.Empty), 5.seconds) }
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
