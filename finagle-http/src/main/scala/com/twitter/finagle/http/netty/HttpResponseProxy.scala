package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{HttpMessage, HttpResponse, HttpResponseStatus}


/** Proxy for HttpResponse.  Used by Response. */
private[finagle] trait HttpResponseProxy extends HttpMessageProxy {
  protected[finagle] def httpResponse: HttpResponse
  protected[finagle] def getHttpResponse(): HttpResponse = httpResponse
  protected[finagle] def httpMessage: HttpMessage = httpResponse

  protected[finagle] def getStatus(): HttpResponseStatus       = httpResponse.getStatus()
  protected[finagle] def setStatus(status: HttpResponseStatus) { httpResponse.setStatus(status) }
}
