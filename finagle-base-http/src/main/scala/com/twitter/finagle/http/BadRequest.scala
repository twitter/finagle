package com.twitter.finagle.http

import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpMethod, HttpRequest, HttpVersion}

private[finagle] case class BadHttpRequest(
  httpVersion: HttpVersion, method: HttpMethod, uri: String, exception: Throwable)
  extends DefaultHttpRequest(httpVersion, method, uri)

object BadHttpRequest {
  def apply(exception: Throwable): BadHttpRequest =
    new BadHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/bad-http-request", exception)
}

private[finagle] sealed trait BadReq
private[finagle] trait ContentTooLong extends BadReq
private[finagle] trait UriTooLong extends BadReq
private[finagle] trait HeaderFieldsTooLarge extends BadReq

private[http] case class BadRequest(httpRequest: HttpRequest, exception: Throwable)
  extends Request with BadReq {
  lazy val remoteSocketAddress = new InetSocketAddress(0)
}

private[finagle] object BadRequest {

  def apply(msg: BadHttpRequest): BadRequest =
    new BadRequest(msg, msg.exception)

  def apply(exn: Throwable): BadRequest = {
    val msg = new BadHttpRequest(
      HttpVersion.HTTP_1_0,
      HttpMethod.GET,
      "/bad-http-request",
      exn
    )

    apply(msg)
  }

  def contentTooLong(msg: BadHttpRequest): BadRequest with ContentTooLong =
    new BadRequest(msg, msg.exception) with ContentTooLong

  def contentTooLong(exn: Throwable): BadRequest with ContentTooLong = {
    val msg = new BadHttpRequest(
      HttpVersion.HTTP_1_0,
      HttpMethod.GET,
      "/bad-http-request",
      exn
    )
    contentTooLong(msg)
  }

  def uriTooLong(msg: BadHttpRequest): BadRequest with UriTooLong =
    new BadRequest(msg, msg.exception) with UriTooLong

  def uriTooLong(exn: Throwable): BadRequest with UriTooLong = {
    val msg = new BadHttpRequest(
      HttpVersion.HTTP_1_0,
      HttpMethod.GET,
      "/bad-http-request",
      exn
    )
    uriTooLong(msg)
  }

  def headerTooLong(msg: BadHttpRequest): BadRequest with HeaderFieldsTooLarge  =
    new BadRequest(msg, msg.exception) with HeaderFieldsTooLarge

  def headerTooLong(exn: Throwable): BadRequest with HeaderFieldsTooLarge = {
    val msg = new BadHttpRequest(
      HttpVersion.HTTP_1_0,
      HttpMethod.GET,
      "/bad-http-request",
      exn
    )
    headerTooLong(msg)
  }
}
