package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http.InvalidUriException
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.{
  DefaultHttpRequest,
  HttpMethod,
  HttpRequest,
  HttpResponse,
  HttpVersion
}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class UriValidatorHandlerTest extends AnyFunSuite with MockitoSugar {

  test("Accepts valid URI") {
    val channel = new EmbeddedChannel(UriValidatorHandler)
    val msg = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/abc.jpg")
    assert(channel.writeInbound(msg))
    assert(channel.readInbound[HttpRequest].uri() == "/abc.jpg")
  }

  test("Marks decoder failure for invalid URI (non-ascii chars)") {
    val channel = new EmbeddedChannel(UriValidatorHandler)
    val msg = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/DSC02175拷貝.jpg")
    assert(channel.writeInbound(msg))
    assert(channel.readInbound[HttpResponse].decoderResult().isFailure)
  }

  test("Write exception for invalid URI (non-ascii chars)") {
    val channel = new EmbeddedChannel(UriValidatorHandler)
    val msg = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/DSC02175拷貝.jpg")
    intercept[InvalidUriException] {
      channel.writeOutbound(msg)
    }
  }

  test("Marks decoder failure for invalid URI (encoding)") {
    val channel = new EmbeddedChannel(UriValidatorHandler)
    val msg = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/1%%.jpg")
    assert(channel.writeInbound(msg))
    assert(channel.readInbound[HttpResponse].decoderResult().isFailure)
  }

  test("Write exception for invalid URI (encoding)") {
    val channel = new EmbeddedChannel(UriValidatorHandler)
    val msg = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/1%%.jpg")
    intercept[InvalidUriException] {
      channel.writeOutbound(msg)
    }
  }

}
