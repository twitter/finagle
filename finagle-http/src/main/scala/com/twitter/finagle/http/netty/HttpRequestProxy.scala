package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{HttpMessage, HttpRequest, HttpMethod}


/** Proxy for HttpRequest.  Used by Request. */
private[finagle] trait HttpRequestProxy extends HttpMessageProxy {
  protected[finagle] def httpRequest: HttpRequest
  protected[finagle] def getHttpRequest(): HttpRequest = httpRequest
  protected[finagle] def httpMessage: HttpMessage = httpRequest

  protected[finagle] def getMethod(): HttpMethod       = httpRequest.getMethod
  protected[finagle] def setMethod(method: HttpMethod) { httpRequest.setMethod(method) }
  protected[finagle] def getUri(): String              = httpRequest.getUri()
  protected[finagle] def setUri(uri: String)           { httpRequest.setUri(uri) }
}
