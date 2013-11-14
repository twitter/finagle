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

  @deprecated("deprecated in netty", "6.8.0")
  def getHeader(name: String): String                 = httpMessage.getHeader(name)
  @deprecated("deprecated in netty", "6.8.0")
  def getHeaders(name: String): JList[String]         = httpMessage.getHeaders(name)
  @deprecated("deprecated in netty", "6.8.0")
  def getHeaders(): JList[JMap.Entry[String, String]] = httpMessage.getHeaders()
  @deprecated("deprecated in netty", "6.8.0")
  def containsHeader(name: String): Boolean           = httpMessage.containsHeader(name)
  @deprecated("deprecated in netty", "6.8.0")
  def getHeaderNames(): JSet[String]                  = httpMessage.getHeaderNames()
  @deprecated("deprecated in netty", "6.8.0")
  def addHeader(name: String, value: Object)          { httpMessage.addHeader(name, value) }
  @deprecated("deprecated in netty", "6.8.0")
  def setHeader(name: String, value: Object)          { httpMessage.setHeader(name, value) }
  @deprecated("deprecated in netty", "6.8.0")
  def setHeader(name: String, values: JIterable[_])   { httpMessage.setHeader(name, values) }
  @deprecated("deprecated in netty", "6.8.0")
  def removeHeader(name: String)                      { httpMessage.removeHeader(name) }
  @deprecated("deprecated in netty", "6.8.0")
  def clearHeaders()                                  { httpMessage.clearHeaders() }

  def getProtocolVersion(): HttpVersion        = httpMessage.getProtocolVersion()
  def setProtocolVersion(version: HttpVersion) { httpMessage.setProtocolVersion(version) }

  def headers(): HttpHeaders = httpMessage.headers()

  def getContent(): ChannelBuffer        = httpMessage.getContent()
  def setContent(content: ChannelBuffer) { httpMessage.setContent(content) }

  def isChunked: Boolean           = httpMessage.isChunked()
  def setChunked(chunked: Boolean) { httpMessage.setChunked(chunked) }
}
