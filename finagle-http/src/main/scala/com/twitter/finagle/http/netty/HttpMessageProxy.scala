package com.twitter.finagle.http.netty

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http.{HttpHeaders, HttpMessage, HttpVersion}
import java.lang.{Iterable => JIterable}
import java.util.{List => JList, Map => JMap, Set => JSet}


/** Proxy for HttpMessage.  Used by Request and Response. */
trait HttpMessageProxy extends HttpMessage with Proxy {
  def httpMessage: HttpMessage
  def getHttpMessage(): HttpMessage = httpMessage
  def self = httpMessage

  def getProtocolVersion(): HttpVersion        = httpMessage.getProtocolVersion()
  def setProtocolVersion(version: HttpVersion) { httpMessage.setProtocolVersion(version) }

  def headers(): HttpHeaders = httpMessage.headers()

  def getContent(): ChannelBuffer        = httpMessage.getContent()
  def setContent(content: ChannelBuffer) { httpMessage.setContent(content) }

  def isChunked: Boolean           = httpMessage.isChunked()
  def setChunked(chunked: Boolean) { httpMessage.setChunked(chunked) }
}
