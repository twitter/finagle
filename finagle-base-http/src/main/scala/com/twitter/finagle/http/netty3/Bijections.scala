package com.twitter.finagle.http.netty3

import com.twitter.finagle.http._
import com.twitter.finagle.http.cookie.SameSite
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import org.jboss.netty.handler.codec.http.{Cookie => NettyCookie, _}

private[finagle] trait Injection[A, B] {
  def apply(a: A): B
}

object Bijections {
  def from[A, B](a: A)(implicit I: Injection[A, B]): B = I.apply(a)

  // Version

  implicit val versionToNettyInjection = new Injection[Version, HttpVersion] {
    def apply(v: Version): HttpVersion = versionToNetty(v)
  }

  def versionToNetty(v: Version): HttpVersion = v match {
    case Version.Http11 => HttpVersion.HTTP_1_1
    case Version.Http10 => HttpVersion.HTTP_1_0
  }

  // Note: netty 3's HttpVersion allows arbitrary protocol names so the bijection
  // here is a lie since finagle-http's Version can only represent HTTP/1.0 and HTTP/1.1.
  // However, netty 3 only decodes HTTP/1.0 and HTTP/1.1 messages so whatever came over
  // the wire at least looks like HTTP/1.x, so we take a guess in the base case.
  implicit val versionFromNettyInjection = new Injection[HttpVersion, Version] {
    def apply(v: HttpVersion): Version = versionFromNetty(v)
  }

  def versionFromNetty(v: HttpVersion): Version = v match {
    case HttpVersion.HTTP_1_1 => Version.Http11
    case HttpVersion.HTTP_1_0 => Version.Http10
    case _ => Version.Http11
  }

  // Method

  implicit val methodToNettyInjection = new Injection[Method, HttpMethod] {
    def apply(m: Method): HttpMethod = methodToNetty(m)
  }

  def methodToNetty(m: Method): HttpMethod =
    HttpMethod.valueOf(m.toString)

  implicit val methodFromNettyInjection = new Injection[HttpMethod, Method] {
    def apply(m: HttpMethod): Method = methodFromNetty(m)
  }

  def methodFromNetty(m: HttpMethod): Method = Method(m.getName)

  // Status

  implicit val statusToNettyInjection = new Injection[Status, HttpResponseStatus] {
    def apply(s: Status): HttpResponseStatus = statusToNetty(s)
  }

  def statusToNetty(s: Status): HttpResponseStatus = HttpResponseStatus.valueOf(s.code)

  implicit val statusFromNettyInjection = new Injection[HttpResponseStatus, Status] {
    def apply(s: HttpResponseStatus): Status = statusFromNetty(s)
  }

  def statusFromNetty(s: HttpResponseStatus): Status = Status.fromCode(s.getCode)

  // Cookie

  private[http] implicit val cookieToNettyInjection: Injection[Cookie, NettyCookie] =
    new Injection[Cookie, NettyCookie] {
      def apply(c: Cookie): NettyCookie = cookieToNetty(c)
    }

  private[http] def cookieToNetty(c: Cookie): NettyCookie = {
    val nc = new DefaultCookie(c.name, c.value)

    nc.setDomain(c.domain)
    nc.setPath(c.path)
    nc.setMaxAge(c.maxAge.inSeconds)
    nc.setSecure(c.secure)
    nc.setHttpOnly(c.httpOnly)
    nc
  }

  private[http] implicit val cookieFromNettyInjection: Injection[NettyCookie, Cookie] =
    new Injection[NettyCookie, Cookie] {
      def apply(nc: NettyCookie): Cookie = cookieFromNetty(nc)
    }

  private[http] def cookieFromNetty(nc: NettyCookie): Cookie = nc match {
    case sc: SameSiteSupportingCookie =>
      new Cookie(sc).sameSite(SameSite.fromString(sc.getSameSite))
    case _ =>
      new Cookie(nc)
  }


  // Request

  implicit val requestToNettyInjection = new Injection[Request, HttpRequest] {
    def apply(r: Request): HttpRequest = requestToNetty(r)
  }

  def requestToNetty(r: Request): HttpRequest = {
    val nettyRequest =
      new DefaultHttpRequest(versionToNetty(r.version), methodToNetty(r.method), r.uri)
    copyHeadersAndContentToNetty(r, nettyRequest)
    nettyRequest
  }

  implicit val requestFromNettyInjection = new Injection[HttpRequest, Request] {
    def apply(r: HttpRequest): Request = requestFromNetty(r)
  }

  def requestFromNetty(r: HttpRequest): Request = {
    val req =
      Request(versionFromNetty(r.getProtocolVersion), methodFromNetty(r.getMethod), r.getUri)
    copyHeadersAndContentFromNetty(r, req)
    req
  }

  // Response

  implicit val responseFromNettyInjection = new Injection[HttpResponse, Response] {
    def apply(r: HttpResponse): Response = responseFromNetty(r)
  }

  def responseFromNetty(r: HttpResponse): Response = {
    val resp = Response(versionFromNetty(r.getProtocolVersion), statusFromNetty(r.getStatus))
    copyHeadersAndContentFromNetty(r, resp)
    resp
  }

  implicit val responseToNettyInjection = new Injection[Response, HttpResponse] {
    def apply(r: Response): HttpResponse = responseToNetty(r)
  }

  def responseToNetty(r: Response): HttpResponse = {
    val nettyResponse = new DefaultHttpResponse(versionToNetty(r.version), statusToNetty(r.status))
    copyHeadersAndContentToNetty(r, nettyResponse)
    nettyResponse
  }

  def copyHeadersAndContentFromNetty(httpMessage: HttpMessage, message: Message): Unit = {
    copyHeadersFromNetty(httpMessage.headers, message.headerMap)
    if (httpMessage.isChunked) {
      message.setChunked(true)
    } else if (httpMessage.getContent.readable()) { // we have static content
      message.content = ChannelBufferBuf.newOwned(httpMessage.getContent)
    }
  }

  def copyHeadersAndContentToNetty(message: Message, httpMessage: HttpMessage): Unit = {
    copyHeadersToNetty(message.headerMap, httpMessage.headers)
    if (message.isChunked) {
      httpMessage.setChunked(true)
    } else if (!message.content.isEmpty) {
      // We duplicate in case this was a ChannelBufferBuf and we got a ref to the underlying ChannelBuffer
      httpMessage.setContent(BufChannelBuffer(message.content).duplicate())
    }
  }

  private def copyHeadersFromNetty(httpHeaders: HttpHeaders, headers: HeaderMap): Unit = {
    val it = httpHeaders.iterator()
    while (it.hasNext) {
      val e = it.next()
      headers.add(e.getKey, e.getValue)
    }
  }

  private def copyHeadersToNetty(headers: HeaderMap, httpHeaders: HttpHeaders): Unit = {
    headers.foreach { case (k, v) => httpHeaders.add(k, v) }
  }
}
