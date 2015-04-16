package com.twitter.finagle.httpx.netty

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http.{HttpHeaders, HttpMessage, HttpVersion}
import java.lang.{Iterable => JIterable}
import java.util.{List => JList, Map => JMap, Set => JSet}


/** Proxy for HttpMessage.  Used by Request and Response. */
private[finagle] trait HttpMessageProxy extends Proxy {
  protected[finagle] def httpMessage: HttpMessage
  protected[finagle] def getHttpMessage(): HttpMessage = httpMessage
  def self = httpMessage

  protected[finagle] def getProtocolVersion(): HttpVersion =
    httpMessage.getProtocolVersion()

  protected[finagle] def setProtocolVersion(version: HttpVersion): Unit =
    httpMessage.setProtocolVersion(version)

  protected[finagle] def headers(): HttpHeaders =
    httpMessage.headers()

  protected[finagle] def getContent(): ChannelBuffer =
    httpMessage.getContent()

  protected[finagle] def setContent(content: ChannelBuffer): Unit =
    httpMessage.setContent(content)

  def isChunked: Boolean = httpMessage.isChunked()

  def setChunked(chunked: Boolean): Unit =
    httpMessage.setChunked(chunked)
}
