package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{HttpMessage, HttpRequest, HttpMethod}


/** Proxy for HttpRequest.  Used by Request. */
trait HttpRequestProxy extends HttpRequest with HttpMessageProxy {
  def httpRequest: HttpRequest
  def getHttpRequest(): HttpRequest = httpRequest
  def httpMessage: HttpMessage = httpRequest

  def getMethod(): HttpMethod       = httpRequest.getMethod
  def setMethod(method: HttpMethod) { httpRequest.setMethod(method) }
  def getUri(): String              = httpRequest.getUri()
  def setUri(uri: String)           { httpRequest.setUri(uri) }
}
