package com.twitter.finagle.http.netty

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http.{HttpMessage, HttpVersion}
import java.lang.{Iterable => JIterable}
import java.util.{List => JList, Map => JMap, Set => JSet}


/** Proxy for HttpMessage.  Used by Request and Response. */
trait HttpMessageProxy extends HttpMessage with Proxy {
  def httpMessage: HttpMessage
  def getHttpMessage(): HttpMessage = httpMessage
  def self = httpMessage

  def getHeader(name: String): String                 = httpMessage.getHeader(name)
  def getHeaders(name: String): JList[String]         = httpMessage.getHeaders(name)
  def getHeaders(): JList[JMap.Entry[String, String]] = httpMessage.getHeaders()
  def containsHeader(name: String): Boolean           = httpMessage.containsHeader(name)
  def getHeaderNames(): JSet[String]                  = httpMessage.getHeaderNames()
  def addHeader(name: String, value: Object)          { httpMessage.addHeader(name, value) }
  def setHeader(name: String, value: Object)          { httpMessage.setHeader(name, value) }
  def setHeader(name: String, values: JIterable[_])   { httpMessage.setHeader(name, values) }
  def removeHeader(name: String)                      { httpMessage.removeHeader(name) }
  def clearHeaders()                                  { httpMessage.clearHeaders() }

  def getProtocolVersion(): HttpVersion        = httpMessage.getProtocolVersion()
  def setProtocolVersion(version: HttpVersion) { httpMessage.setProtocolVersion(version) }

  def getContent(): ChannelBuffer        = httpMessage.getContent()
  def setContent(content: ChannelBuffer) { httpMessage.setContent(content) }

  @deprecated("deprecated in netty", "6.1.5")
  def getContentLength(): Long                   = httpMessage.getContentLength()
  @deprecated("deprecated in netty", "6.1.5")
  def getContentLength(defaultValue: Long): Long = httpMessage.getContentLength(defaultValue)

  def isChunked: Boolean           = httpMessage.isChunked()
  def setChunked(chunked: Boolean) { httpMessage.setChunked(chunked) }

  @deprecated("deprecated in netty", "6.1.5")
  def isKeepAlive: Boolean = httpMessage.isKeepAlive()
}
