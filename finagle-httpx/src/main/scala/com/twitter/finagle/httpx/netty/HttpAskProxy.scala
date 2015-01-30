package com.twitter.finagle.httpx.netty

import org.jboss.netty.handler.codec.http.{HttpMessage, HttpRequest => HttpAsk, HttpMethod}


/** Proxy for HttpAsk.  Used by Ask. */
private[finagle] trait HttpAskProxy extends HttpMessageProxy {
  protected[finagle] def httpAsk: HttpAsk
  protected[finagle] def getHttpAsk(): HttpAsk = httpAsk
  protected[finagle] def httpMessage: HttpMessage = httpAsk

  protected[finagle] def getMethod(): HttpMethod       = httpAsk.getMethod
  protected[finagle] def setMethod(method: HttpMethod) { httpAsk.setMethod(method) }
  protected[finagle] def getUri(): String              = httpAsk.getUri()
  protected[finagle] def setUri(uri: String)           { httpAsk.setUri(uri) }
}
