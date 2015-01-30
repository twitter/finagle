package com.twitter.finagle.http.netty

import org.jboss.netty.handler.codec.http.{HttpMessage, HttpRequest => HttpAsk, HttpMethod}


/** Proxy for HttpAsk.  Used by Ask. */
trait HttpAskProxy extends HttpAsk with HttpMessageProxy {
  def httpAsk: HttpAsk
  def getHttpAsk(): HttpAsk = httpAsk
  def httpMessage: HttpMessage = httpAsk

  def getMethod(): HttpMethod       = httpAsk.getMethod
  def setMethod(method: HttpMethod) { httpAsk.setMethod(method) }
  def getUri(): String              = httpAsk.getUri()
  def setUri(uri: String)           { httpAsk.setUri(uri) }
}
